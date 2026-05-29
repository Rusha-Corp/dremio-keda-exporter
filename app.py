"""Dremio metrics exporter for KEDA-driven autoscaling.

Exposes Dremio application-aware metrics as a JSON HTTP endpoint that
KEDA's metrics-api scaler polls to drive executor StatefulSet scaling.

Metrics exposed at GET /json
─────────────────────────────────────────────────────────────────────────────
 executor_desired_small  Desired replica count for dremio-executor-small.
 executor_desired_large  Desired replica count for dremio-executor-large.
 active_small_jobs       Jobs completed recently on SMALL queue (informational).
 active_large_jobs       Jobs completed recently on LARGE queue (informational).
 registered_executors    Executors visible in sys.nodes (informational).

Scale-down gate logic (per tier)
──────────────────────────────────
The exporter never knows about in-flight jobs directly (/apiv2/jobs only
returns terminal jobs). Instead it uses three signals, highest priority first:

 1. Scale-request annotation  dremio.io/scale-requested-at on the StatefulSet,
                               written by ElasticResourceAllocator when it needs
                               to cold-start executors. Held for SCALE_DOWN_GRACE_SECS.
 2. Ready-replica window       From the moment readyReplicas first goes > 0, hold
                               desired = spec_replicas for SCALE_DOWN_GRACE_SECS.
                               Covers any in-flight job that started after cold-start.
 3. Recent-job history         /apiv2/jobs endTime recency — any job whose endTime
                               falls within SCALE_DOWN_GRACE_SECS resets the idle
                               timer. /api/v3/job/{id} gives queueName for tier routing.

When any signal fires, the hold logic is: return current spec_replicas (never
force it up or down beyond what's already running). KEDA's own cooldownPeriod
provides an additional buffer after desired first drops to 0.
"""

import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError

from flask import Flask, jsonify

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
DREMIO_URL = os.environ.get(
    "DREMIO_URL", "http://dremio-coordinator-hs.dremio.svc.cluster.local:9047"
)
DREMIO_USERNAME = os.environ.get("DREMIO_USERNAME", "")
DREMIO_PASSWORD = os.environ.get("DREMIO_PASSWORD", "")
SCALE_DOWN_GRACE_SECS = int(os.environ.get("SCALE_DOWN_GRACE_SECS", "1800"))

_LARGE_QUEUES = {"LARGE", "REFLECTION_LARGE"}
_SMALL_QUEUES = {"SMALL", "REFLECTION_SMALL", "LOW_COST"}

# ── Dremio REST client ─────────────────────────────────────────────────────────


class DremioClient:
    """Thin Dremio REST client with session token caching (1h TTL)."""

    def __init__(self, base_url: str, username: str, password: str):
        self._url = base_url.rstrip("/")
        self._username = username
        self._password = password
        self._token: Optional[str] = None
        self._token_ts: float = 0

    def _ensure_token(self):
        if self._token and (time.time() - self._token_ts) < 3600:
            return
        payload = json.dumps({"userName": self._username, "password": self._password}).encode()
        req = Request(
            f"{self._url}/apiv2/login",
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        self._token = json.loads(urlopen(req, timeout=10).read())["token"]
        self._token_ts = time.time()

    def _auth_headers(self) -> dict:
        self._ensure_token()
        return {"Authorization": f"_dremio{self._token}", "Content-Type": "application/json"}

    def list_jobs(self, limit: int = 200) -> list[dict]:
        """Return recent jobs (terminal and non-terminal) from job history."""
        headers = self._auth_headers()
        try:
            resp = urlopen(
                Request(f"{self._url}/apiv2/jobs?limit={limit}", headers=headers), timeout=15
            )
            return json.loads(resp.read()).get("jobs", [])
        except HTTPError as exc:
            logger.warning("apiv2/jobs HTTP %s", exc.code)
            raise

    def get_job_queue(self, job_id: str) -> str:
        """Return queueName for a job ('SMALL', 'LARGE', etc.) or '' on failure."""
        headers = self._auth_headers()
        try:
            resp = urlopen(
                Request(f"{self._url}/api/v3/job/{job_id}", headers=headers), timeout=5
            )
            return json.loads(resp.read()).get("queueName", "")
        except Exception as exc:
            logger.debug("get_job_queue(%s) failed: %s", job_id, exc)
            return ""

    def count_nodes(self) -> int:
        """Count registered executors (informational; requires executors running)."""
        headers = self._auth_headers()
        try:
            sql = "SELECT COUNT(*) AS cnt FROM sys.nodes WHERE is_executor = true"
            resp = urlopen(
                Request(
                    f"{self._url}/api/v3/sql",
                    data=json.dumps({"sql": sql}).encode(),
                    headers=headers,
                ),
                timeout=5,
            )
            job_id = json.loads(resp.read()).get("id")
            if not job_id:
                return 0
            for _ in range(6):
                time.sleep(0.5)
                detail = json.loads(
                    urlopen(Request(f"{self._url}/api/v3/job/{job_id}", headers=headers), timeout=5).read()
                )
                state = detail.get("jobState", "")
                if state == "COMPLETED":
                    rows = json.loads(
                        urlopen(
                            Request(f"{self._url}/api/v3/job/{job_id}/results", headers=headers), timeout=5
                        ).read()
                    ).get("rows", [])
                    return int(rows[0]["cnt"]) if rows else 0
                if state in ("FAILED", "CANCELED"):
                    return 0
            return 0
        except Exception as exc:
            logger.debug("count_nodes failed: %s", exc)
            return 0


# ── Kubernetes state collector ─────────────────────────────────────────────────


class K8sStateCollector:
    """Reads StatefulSet spec replicas, ready replicas, and annotations."""

    def __init__(self):
        self._available = False
        self._namespace = os.environ.get("NAMESPACE", "dremio")
        try:
            from kubernetes import config as k8s_config, client
            k8s_config.load_incluster_config()
            self._apps = client.AppsV1Api()
            self._available = True
        except Exception as exc:
            logger.warning("K8s client not available: %s", exc)

    def get_statefulset_info(self, name: str) -> tuple[int, int, float, int]:
        """Return (spec_replicas, ready_replicas, scale_requested_at_epoch, scale_requested_count).

        scale_requested_at_epoch comes from the dremio.io/scale-requested-at
        annotation written by ElasticResourceAllocator before cold-starting pods.
        Returns 0.0 for the timestamp if the annotation is absent or unparseable.
        scale_requested_count comes from dremio.io/scale-requested-count,
        the desired replica count the coordinator requested. Returns 0 if absent.
        """
        if not self._available:
            return 0, 0, 0.0, 0
        try:
            sts = self._apps.read_namespaced_stateful_set(name, self._namespace)
            spec_replicas = sts.spec.replicas or 0
            ready_replicas = (sts.status.ready_replicas or 0) if sts.status else 0
            annotations = sts.metadata.annotations or {}
            try:
                scale_ts = int(annotations.get("dremio.io/scale-requested-at", "0")) / 1000.0
            except (ValueError, TypeError):
                scale_ts = 0.0
            try:
                scale_count = int(annotations.get("dremio.io/scale-requested-count", "0"))
            except (ValueError, TypeError):
                scale_count = 0
            return spec_replicas, ready_replicas, scale_ts, scale_count
        except Exception as exc:
            logger.warning("Failed to read StatefulSet %s: %s", name, exc)
            return 0, 0, 0.0, 0


# ── Metrics snapshot ───────────────────────────────────────────────────────────


@dataclass
class MetricsSnapshot:
    active_small_jobs: int = 0
    active_large_jobs: int = 0
    active_user_jobs: int = 0
    active_reflection_jobs: int = 0
    registered_executors: int = 0
    executor_desired_small: int = 0
    executor_desired_large: int = 0

    def to_dict(self) -> dict:
        return {
            "active_small_jobs": self.active_small_jobs,
            "active_large_jobs": self.active_large_jobs,
            "active_user_jobs": self.active_user_jobs,
            "active_reflection_jobs": self.active_reflection_jobs,
            "registered_executors": self.registered_executors,
            "executor_desired_small": self.executor_desired_small,
            "executor_desired_large": self.executor_desired_large,
        }


# ── Main metrics collector ─────────────────────────────────────────────────────


class DremioMetricsCollector:
    """Computes executor_desired counts from job history and K8s state."""

    def __init__(self):
        self._dremio = DremioClient(DREMIO_URL, DREMIO_USERNAME, DREMIO_PASSWORD)
        self._k8s = K8sStateCollector()
        self._cache: Optional[MetricsSnapshot] = None

        # Grace timers — 0 means "never seen activity"; grace is considered expired.
        # Populated on first _collect() call from job history.
        self._last_active_small: float = 0.0
        self._last_active_large: float = 0.0

        # Track when each tier's executors first became ready in this window.
        self._small_became_ready_at: Optional[float] = None
        self._large_became_ready_at: Optional[float] = None

        # Cache: job_id → queueName, pruned each cycle.
        self._queue_cache: dict[str, str] = {}

    def get(self) -> MetricsSnapshot:
        return self._cache if self._cache else MetricsSnapshot()

    def refresh(self) -> None:
        snap = self._collect()
        self._cache = snap
        logger.info("Metrics: %s", snap.to_dict())

    def _collect(self) -> MetricsSnapshot:
        snap = MetricsSnapshot()
        now = time.time()
        now_ms = int(now * 1000)
        grace_ms = SCALE_DOWN_GRACE_SECS * 1000

        # ── K8s StatefulSet state ─────────────────────────────────────────
        spec_small, ready_small, scale_req_small, scale_req_small_count = self._k8s.get_statefulset_info("dremio-executor-small")
        spec_large, ready_large, scale_req_large, scale_req_large_count = self._k8s.get_statefulset_info("dremio-executor-large")

        # ── Ready-replica window tracking ─────────────────────────────────
        # From the moment readyReplicas first becomes > 0, we start a grace window.
        # This covers in-flight jobs that started right after executor cold-start,
        # before any job appears in the completed-jobs history.
        if ready_small > 0:
            if self._small_became_ready_at is None:
                self._small_became_ready_at = now
        else:
            self._small_became_ready_at = None

        if ready_large > 0:
            if self._large_became_ready_at is None:
                self._large_became_ready_at = now
        else:
            self._large_became_ready_at = None

        # ── Job history: endTime recency per tier ─────────────────────────
        # /apiv2/jobs returns terminal (COMPLETED/FAILED) jobs only.
        # We scan for jobs whose endTime falls within the grace window, then
        # look up their queueName to classify the tier.
        recently_active_small = False
        recently_active_large = False
        recent_small_jobs = 0
        recent_large_jobs = 0

        try:
            jobs = self._dremio.list_jobs(limit=200)

            # Prune queue cache: drop entries older than 2x grace window.
            cutoff_ms = now_ms - 2 * grace_ms
            self._queue_cache = {
                jid: q for jid, q in self._queue_cache.items()
                if any(j["id"] == jid and j.get("endTime", 0) > cutoff_ms for j in jobs)
            }

            for job in jobs:
                end_time_ms = job.get("endTime", 0)
                if not end_time_ms or (now_ms - end_time_ms) > grace_ms:
                    continue  # outside grace window or no endTime
                jid = job["id"]
                if jid not in self._queue_cache:
                    queue = self._dremio.get_job_queue(jid)
                    if queue:
                        self._queue_cache[jid] = queue
                queue = self._queue_cache.get(jid, "")
                if queue in _LARGE_QUEUES:
                    recently_active_large = True
                    recent_large_jobs += 1
                elif queue in _SMALL_QUEUES or queue == "":
                    recently_active_small = True
                    recent_small_jobs += 1

            snap.active_small_jobs = recent_small_jobs
            snap.active_large_jobs = recent_large_jobs
            snap.active_user_jobs = recent_small_jobs + recent_large_jobs
            snap.registered_executors = self._dremio.count_nodes()
        except TimeoutError:
            logger.warning("Dremio REST timed out — fail-open, holding current desired")
            # On timeout keep existing timers alive; don't reset to 0.
            recently_active_small = True
            recently_active_large = True
        except Exception as exc:
            logger.warning("Dremio unavailable: %s", exc)

        # ── Update grace timers from all signals ──────────────────────────
        # Signal 1: recent completed jobs in history
        if recently_active_small:
            self._last_active_small = now
        if recently_active_large:
            self._last_active_large = now

        # Signal 2: ready-replica window (executor running, might have in-flight job)
        if self._small_became_ready_at and (now - self._small_became_ready_at) < SCALE_DOWN_GRACE_SECS:
            self._last_active_small = now
        if self._large_became_ready_at and (now - self._large_became_ready_at) < SCALE_DOWN_GRACE_SECS:
            self._last_active_large = now

        # Signal 3: scale-request annotation written by ElasticResourceAllocator.
        # The scale-requested-count annotation carries the desired replica count.
        # We propagate it to KEDA via desired=N so KEDA applies spec.replicas=N
        # instead of overriding it. This makes KEDA the sole authority on spec.replicas
        # and eliminates the dual-write race condition.
        if scale_req_small and (now - scale_req_small) < SCALE_DOWN_GRACE_SECS:
            self._last_active_small = now
            spec_small = max(spec_small, scale_req_small_count)
        if scale_req_large and (now - scale_req_large) < SCALE_DOWN_GRACE_SECS:
            self._last_active_large = now
            spec_large = max(spec_large, scale_req_large_count)

        # ── Compute desired replica counts ────────────────────────────────
        snap.executor_desired_small = self._compute_desired("small", spec_small, self._last_active_small)
        snap.executor_desired_large = self._compute_desired("large", spec_large, self._last_active_large)
        return snap

    def _compute_desired(self, tier: str, spec_replicas: int, last_active: float) -> int:
        """Hold at spec_replicas while within grace; return 0 when idle past grace.

        spec_replicas may already carry the requested count from the
        scale-requested-count annotation (applied in Signal 3). KEDA is the
        sole authority on spec.replicas — this method never writes to K8s.
        Holding spec_replicas prevents KEDA from scaling to 0 while jobs run.
        """
        secs_idle = time.time() - last_active
        if last_active > 0 and secs_idle < SCALE_DOWN_GRACE_SECS:
            if spec_replicas > 0:
                logger.info(
                    "%s tier idle %.0fs/%ds, holding at %d",
                    tier, secs_idle, SCALE_DOWN_GRACE_SECS, spec_replicas,
                )
            return spec_replicas  # hold — could be 0 if not yet scaled up
        if spec_replicas > 0:
            logger.info("%s tier idle %.0fs (past grace), scaling to 0", tier, secs_idle)
        return 0


# ── Singleton and background thread ───────────────────────────────────────────
_collector = DremioMetricsCollector()
_bg_started = threading.Event()


def _bg_collect_loop() -> None:
    while True:
        try:
            _collector.refresh()
        except Exception as exc:
            logger.warning("Background collect error: %s", exc)
        time.sleep(15)


@app.before_request
def _ensure_bg_thread() -> None:
    if not _bg_started.is_set():
        _bg_started.set()
        # Prime the cache synchronously before starting background loop
        # so the first KEDA poll gets real data, not a zero snapshot.
        try:
            _collector.refresh()
        except Exception as exc:
            logger.warning("Initial collect failed: %s", exc)
        t = threading.Thread(target=_bg_collect_loop, daemon=True)
        t.start()


@app.route("/json")
def metrics_json():
    """KEDA metrics-api endpoint. Always returns immediately from cache."""
    return jsonify(_collector.get().to_dict())


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
