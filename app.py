"""Dremio metrics exporter for KEDA-driven autoscaling.

Exposes Dremio application-aware metrics as a JSON HTTP endpoint that
KEDA's metrics-api scaler polls to replace the autoscale/autostop CronJobs.

Metrics exposed at GET /json
─────────────────────────────────────────────────────────────────────────────
 active_user_jobs     Running/queued jobs from human users and platform
                      service accounts (excludes $dremio$, ACCELERATION,
                      dremio.ops — the ops/system accounts).
 active_small_jobs    User jobs with planner_estimated_cost <= threshold.
 active_large_jobs    User jobs with planner_estimated_cost > threshold.
 active_reflection_jobs  Running jobs from system accounts ($dremio$, etc.).
 registered_executors Executors registered with Dremio coordinator.
 executor_desired_small  Desired StatefulSet replica count for small tier.
 executor_desired_large  Desired StatefulSet replica count for large tier.

Scale gate logic (per tier)
────────────────────────────
SMALL tier (user + reflection jobs):
  • active_small_jobs > 0 or reflection_jobs > 0 → hold at current
  • idle but within SCALE_DOWN_GRACE_SECS → hold at current (drain fragments)
  • idle past grace period → 0 (scale to zero)

LARGE tier (user jobs only):
  • active_large_jobs > 0 → hold at current
  • idle but within SCALE_DOWN_GRACE_SECS → hold at current (drain fragments)
  • idle past grace period → 0 (scale to zero)
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
DREMIO_LIVENESS_URL = os.environ.get(
    "DREMIO_LIVENESS_URL",
    "http://dremio-coordinator-liveness.dremio.svc.cluster.local:45679/metrics",
)
DREMIO_USERNAME = os.environ.get("DREMIO_USERNAME", "")
DREMIO_PASSWORD = os.environ.get("DREMIO_PASSWORD", "")
MIN_EXECUTORS = int(os.environ.get("MIN_EXECUTORS", "0"))
MAX_EXECUTORS = int(os.environ.get("MAX_EXECUTORS", "4"))
SCALE_DOWN_GRACE_SECS = int(os.environ.get("SCALE_DOWN_GRACE_SECS", "120"))
TERMINAL_DRAIN_SECS = int(os.environ.get("TERMINAL_DRAIN_SECS", "120"))
# Early-exit pagination: stop after this many pages (hard OOM guard)
MAX_JOB_PAGES = int(os.environ.get("MAX_JOB_PAGES", "10"))
# Early-exit pagination: ignore jobs older than this many seconds (active jobs must be recent)
JOB_LOOKBACK_SECS = int(os.environ.get("JOB_LOOKBACK_SECS", "7200"))

_SYSTEM_USERS = ["$dremio$", "ACCELERATION", "dremio.ops"]
_TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELED", "CANCELLATION_REQUESTED"}

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

    def list_jobs(self) -> list[dict]:
        """Return only active (non-terminal) jobs started within JOB_LOOKBACK_SECS.

        Dremio returns jobs newest-first. We stop pagination as soon as we
        encounter a job whose startTime is older than the lookback window — any
        currently-running job must have started within that window. A hard cap
        of MAX_JOB_PAGES prevents OOMKills when the historical job set is huge
        (e.g. 8,300+ pages).

        Only non-terminal jobs are returned, so callers never need to re-filter.
        """
        self._ensure_token()
        headers = {
            "Authorization": f"_dremio{self._token}",
            "Content-Type": "application/json",
        }
        # Dremio timestamps are Unix milliseconds
        cutoff_ms = (time.time() - JOB_LOOKBACK_SECS) * 1000
        url: Optional[str] = f"{self._url}/apiv2/jobs"
        active_jobs: list[dict] = []
        page = 0
        try:
            while url and page < MAX_JOB_PAGES:
                resp = urlopen(Request(url, headers=headers), timeout=30)
                data = json.loads(resp.read())
                stop_early = False
                for job in data.get("jobs", []):
                    start_time = job.get("startTime") or 0
                    if 0 < start_time < cutoff_ms:
                        stop_early = True  # everything after this is even older
                        break
                    if not job.get("isComplete") and job.get("state", "") not in _TERMINAL_STATES:
                        active_jobs.append(job)
                if stop_early:
                    logger.debug(
                        "list_jobs: stopping at page %d — job age exceeds %ds lookback",
                        page, JOB_LOOKBACK_SECS,
                    )
                    break
                next_path = data.get("next")
                # Dremio's "next" uses /jobs/? but the API endpoint is /apiv2/jobs;
                # extract only the query string to build the correct next-page URL.
                url = (
                    f"{self._url}/apiv2/jobs?{next_path.split('?', 1)[1]}"
                    if next_path and "?" in next_path
                    else None
                )
                page += 1
        except HTTPError as exc:
            logger.warning("apiv2/jobs HTTP %s", exc.code)
            raise
        logger.debug("list_jobs: %d active jobs across %d page(s)", len(active_jobs), page)
        return active_jobs


class DremioLivenessClient:
    """Scrapes Dremio liveness /metrics endpoint for gauge values."""

    def __init__(self, liveness_url: str):
        self._url = liveness_url.rstrip("/")

    def get_desired(self) -> tuple[int, int]:
        """Get elastic_desired_small and elastic_desired_large from Prometheus metrics."""
        resp = urlopen(self._url, timeout=5).read().decode()
        small = large = 0
        for line in resp.splitlines():
            if line.startswith("elastic_desired_small "):
                small = int(float(line.split()[1]))
            elif line.startswith("elastic_desired_large "):
                large = int(float(line.split()[1]))
        return small, large


# ── Metrics collector ──────────────────────────────────────────────────────────


@dataclass
class MetricsSnapshot:
    active_user_jobs: int = 0
    active_small_jobs: int = 0
    active_large_jobs: int = 0
    active_reflection_jobs: int = 0
    registered_executors: int = 0
    executor_desired_small: int = MIN_EXECUTORS
    executor_desired_large: int = MIN_EXECUTORS

    def to_dict(self) -> dict:
        return {
            "active_user_jobs": self.active_user_jobs,
            "active_small_jobs": self.active_small_jobs,
            "active_large_jobs": self.active_large_jobs,
            "active_reflection_jobs": self.active_reflection_jobs,
            "registered_executors": self.registered_executors,
            "executor_desired_small": self.executor_desired_small,
            "executor_desired_large": self.executor_desired_large,
        }


# ── Kubernetes state collector ─────────────────────────────────────────────────


class K8sStateCollector:
    """Fetches current StatefulSet replica counts."""

    def __init__(self):
        try:
            from kubernetes import config as k8s_config, client
            k8s_config.load_incluster_config()
            self._apps = client.AppsV1Api()
            self._namespace = os.environ.get("NAMESPACE", "dremio")
            self._client_available = True
        except Exception as e:
            logger.warning("K8s client not available: %s", e)
            self._client_available = False
            self._namespace = "dremio"

    def get_replicas(self, name: str) -> int:
        """Get current replica count for a StatefulSet."""
        if not self._client_available:
            return 0
        try:
            sts = self._apps.read_namespaced_stateful_set(name, self._namespace)
            return sts.spec.replicas or 0
        except Exception as e:
            logger.warning("Failed to read statefulset %s: %s", name, e)
            return 0


# ── Main metrics collector ─────────────────────────────────────────────────────


class DremioMetricsCollector:
    """Orchestrates all collectors and computes executor_desired counts."""

    def __init__(self):
        self._dremio = DremioClient(DREMIO_URL, DREMIO_USERNAME, DREMIO_PASSWORD)
        self._liveness = DremioLivenessClient(DREMIO_LIVENESS_URL)
        self._k8s = K8sStateCollector()
        self._cache: Optional[MetricsSnapshot] = None
        self._cache_ts: float = 0
        # Initialize to now so the grace period starts from startup, not epoch
        self._last_active_small: float = time.time()
        self._last_active_large: float = time.time()
        # Track when terminal drain period started (0 = not in drain)
        self._drain_started_small: float = 0.0
        self._drain_started_large: float = 0.0
        # Set to True once drain completes so we keep returning 0 while KEDA
        # is still updating .spec.replicas (cooldown period can be up to 600s).
        # Cleared the moment a new job becomes active again.
        self._scaled_to_zero_small: bool = False
        self._scaled_to_zero_large: bool = False

    def get(self) -> MetricsSnapshot:
        """Return last cached snapshot immediately (never blocks)."""
        return self._cache if self._cache else MetricsSnapshot()

    def refresh(self) -> None:
        """Collect fresh metrics and update cache. Called from background thread."""
        snap = self._collect()
        self._cache = snap
        self._cache_ts = time.time()
        logger.info("Metrics: %s", snap.to_dict())

    def _collect(self) -> MetricsSnapshot:
        snap = MetricsSnapshot()

        # ── Dremio job metrics via REST (no executor needed) ───────────────
        try:
            jobs = self._dremio.list_jobs()
            # list_jobs() already returns only non-terminal jobs within the lookback window
            user_jobs = 0
            reflection_jobs = 0
            for job in jobs:
                user = job.get("user", "")
                if user in _SYSTEM_USERS:
                    reflection_jobs += 1
                else:
                    user_jobs += 1
            snap.active_user_jobs = user_jobs
            snap.active_small_jobs = user_jobs
            snap.active_large_jobs = user_jobs
            snap.active_reflection_jobs = reflection_jobs
        except Exception as exc:
            # Fail open: any error (timeout, connection refused, OOM, etc.) must
            # NOT cause KEDA to see zero active jobs and scale down. Return 99 so
            # the scale-gate holds all tiers at their current replica counts.
            logger.warning("Dremio unavailable: %s — failing open", exc)
            snap.active_user_jobs = 99
            snap.active_small_jobs = 99
            snap.active_large_jobs = 99

        # ── Update last-active timestamps ────────────────────────────────
        now = time.time()
        if snap.active_small_jobs > 0 or snap.active_reflection_jobs > 0:
            self._last_active_small = now
        if snap.active_large_jobs > 0:
            self._last_active_large = now

        # ── Get desired counts from Dremio's liveness metrics ────────────
        try:
            desired_small, desired_large = self._liveness.get_desired()
        except Exception as exc:
            logger.warning("liveness get_desired failed: %s", exc)
            desired_small, desired_large = 0, 0

        # ── Current StatefulSet replica counts ───────────────────────────
        # These are read from the K8s API (no Dremio SQL job created).
        # registered_executors uses the same values as a lightweight alternative
        # to the old count_nodes() SQL query which generated ~17k jobs/day.
        current_small = self._k8s.get_replicas("dremio-executor-small")
        current_large = self._k8s.get_replicas("dremio-executor-large")
        snap.registered_executors = current_small + current_large

        # ── Compute desired counts ───────────────────────────────────────
        snap.executor_desired_small = self._compute_desired_small(
            current_small, snap.active_small_jobs, snap.active_reflection_jobs, desired_small
        )
        snap.executor_desired_large = self._compute_desired_large(
            current_large, snap.active_large_jobs, desired_large
        )
        return snap

    def _compute_desired_small(
        self, current: int, small_jobs: int, reflection_jobs: int, dremio_desired: int
    ) -> int:
        now = time.time()
        if small_jobs > 0 or reflection_jobs > 0:
            self._drain_started_small = 0.0
            self._scaled_to_zero_small = False
            return max(current, max(dremio_desired, 1))
        # Already decided to scale to zero — keep returning 0 until KEDA acts
        # (cooldownPeriod can be hundreds of seconds; .spec.replicas won't drop
        # to 0 until then, so we must not let a non-zero current restart drain).
        if self._scaled_to_zero_small:
            return 0
        secs_idle = now - self._last_active_small
        if current > 0 and secs_idle < SCALE_DOWN_GRACE_SECS:
            logger.info(
                "small tier idle for %.0fs/%ds, holding at %d",
                secs_idle, SCALE_DOWN_GRACE_SECS, current,
            )
            return current
        if current > 0:
            if self._drain_started_small == 0.0:
                self._drain_started_small = now
                logger.info(
                    "small tier idle for %.0fs (past grace), entering drain period (%ds)",
                    secs_idle, TERMINAL_DRAIN_SECS,
                )
            drain_elapsed = now - self._drain_started_small
            if drain_elapsed < TERMINAL_DRAIN_SECS:
                logger.info(
                    "small tier in drain period (%.0fs/%ds), holding at %d",
                    drain_elapsed, TERMINAL_DRAIN_SECS, current,
                )
                return current
            logger.info("small tier drain complete (%.0fs), scaling to 0", drain_elapsed)
            self._drain_started_small = 0.0
            self._scaled_to_zero_small = True
        return 0

    def _compute_desired_large(self, current: int, large_jobs: int, dremio_desired: int) -> int:
        """Compute desired large executor count.

        dremio_desired comes from the liveness /metrics endpoint (elastic_desired_large).
        Dremio's ElasticResourceAllocator does not publish this metric, so it is always 0.
        When large_jobs > 0 we ensure at least 1 large executor is desired regardless.
        """
        now = time.time()
        if large_jobs > 0:
            self._drain_started_large = 0.0
            self._scaled_to_zero_large = False
            # dremio_desired is always 0 (metric not emitted by Dremio) — use 1 as floor
            return max(current, max(dremio_desired, 1))
        # Already decided to scale to zero — keep returning 0 until KEDA acts.
        if self._scaled_to_zero_large:
            return 0
        secs_idle = now - self._last_active_large
        if current > 0 and secs_idle < SCALE_DOWN_GRACE_SECS:
            logger.info(
                "large tier idle for %.0fs/%ds, holding at %d",
                secs_idle, SCALE_DOWN_GRACE_SECS, current,
            )
            return current
        if current > 0:
            if self._drain_started_large == 0.0:
                self._drain_started_large = now
                logger.info(
                    "large tier idle for %.0fs (past grace), entering drain period (%ds)",
                    secs_idle, TERMINAL_DRAIN_SECS,
                )
            drain_elapsed = now - self._drain_started_large
            if drain_elapsed < TERMINAL_DRAIN_SECS:
                logger.info(
                    "large tier in drain period (%.0fs/%ds), holding at %d",
                    drain_elapsed, TERMINAL_DRAIN_SECS, current,
                )
                return current
            logger.info("large tier drain complete (%.0fs), scaling to 0", drain_elapsed)
            self._drain_started_large = 0.0
            self._scaled_to_zero_large = True
        return 0


# ── Singleton ──────────────────────────────────────────────────────────────────
_collector = DremioMetricsCollector()

# ── Background collection thread ───────────────────────────────────────────────
_bg_started = threading.Event()


def _bg_collect_loop() -> None:
    """Runs in a daemon thread inside each gunicorn worker process."""
    while True:
        try:
            _collector.refresh()
        except Exception as exc:
            logger.warning("Background collect error: %s", exc)
        time.sleep(5)


# ── Flask routes ───────────────────────────────────────────────────────────────


@app.before_request
def _ensure_bg_thread() -> None:
    """Start the background collection thread on the first request to this worker."""
    if not _bg_started.is_set():
        _bg_started.set()
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
