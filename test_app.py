"""Tests for the Dremio metrics exporter."""

import json
import time
import unittest
from unittest.mock import MagicMock, patch


def _login_response():
    """Create a mock response that returns a login token."""
    mock = MagicMock()
    mock.read.return_value = json.dumps({"token": "test_token"}).encode()
    return mock


class TestDremioClient(unittest.TestCase):
    """Tests for DremioClient class."""

    def setUp(self):
        """Set up test fixtures."""
        from app import DremioClient

        self.client = DremioClient("http://test:9047", "user", "pass")

    def test_init(self):
        """Test client initialization."""
        self.assertEqual(self.client._url, "http://test:9047")
        self.assertEqual(self.client._username, "user")
        self.assertEqual(self.client._password, "pass")
        self.assertIsNone(self.client._token)
        self.assertEqual(self.client._token_ts, 0)

    @patch("app.urlopen")
    def test_ensure_token(self, mock_urlopen):
        """Test token caching and refresh."""
        mock_urlopen.return_value = _login_response()

        # First call - should fetch token
        self.client._ensure_token()
        self.assertEqual(self.client._token, "test_token")

        # Second call - should use cached token
        mock_urlopen.reset_mock()
        self.client._ensure_token()
        mock_urlopen.assert_not_called()

    @patch("app.urlopen")
    def test_list_jobs_returns_only_active(self, mock_urlopen):
        """list_jobs() must return only non-terminal jobs; terminal jobs are dropped."""
        login_resp = _login_response()
        jobs_resp = MagicMock()
        jobs_resp.read.return_value = json.dumps({
            "jobs": [
                {"id": "1", "user": "alice", "state": "RUNNING", "isComplete": False},
                {"id": "2", "user": "$dremio$", "state": "COMPLETED", "isComplete": True},
                {"id": "3", "user": "bob", "state": "FAILED", "isComplete": True},
            ],
            "next": None,
        }).encode()
        mock_urlopen.side_effect = [login_resp, jobs_resp]

        jobs = self.client.list_jobs()
        # Only the RUNNING job should be returned; COMPLETED and FAILED are dropped
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["user"], "alice")

    @patch("app.urlopen")
    def test_list_jobs_empty(self, mock_urlopen):
        """Test empty job list."""
        login_resp = _login_response()
        jobs_resp = MagicMock()
        jobs_resp.read.return_value = json.dumps(
            {"jobs": [], "next": None}
        ).encode()
        mock_urlopen.side_effect = [login_resp, jobs_resp]

        jobs = self.client.list_jobs()
        self.assertEqual(jobs, [])

    @patch("app.urlopen")
    def test_list_jobs_follows_pagination(self, mock_urlopen):
        """list_jobs follows pagination pages and fixes Dremio's wrong /jobs/? next URL."""
        now_ms = int(time.time() * 1000)
        login_resp = _login_response()
        page1_resp = MagicMock()
        page1_resp.read.return_value = json.dumps({
            "jobs": [{"id": "1", "user": "alice", "state": "RUNNING",
                      "isComplete": False, "startTime": now_ms}],
            "next": "/jobs/?offset=100&limit=100",
        }).encode()
        page2_resp = MagicMock()
        page2_resp.read.return_value = json.dumps({
            "jobs": [
                {"id": "2", "user": "bob", "state": "RUNNING",
                 "isComplete": False, "startTime": now_ms - 60_000},
                {"id": "3", "user": "carol", "state": "RUNNING",
                 "isComplete": False, "startTime": now_ms - 120_000},
            ],
            "next": None,
        }).encode()
        mock_urlopen.side_effect = [login_resp, page1_resp, page2_resp]

        jobs = self.client.list_jobs()
        self.assertEqual(len(jobs), 3)
        self.assertEqual(jobs[0]["user"], "alice")
        self.assertEqual(jobs[2]["user"], "carol")
        # Second jobs call must use /apiv2/jobs, not /jobs/
        second_call_url = mock_urlopen.call_args_list[2][0][0].full_url
        self.assertIn("/apiv2/jobs", second_call_url)
        self.assertIn("offset=100", second_call_url)

    @patch("app.urlopen")
    def test_list_jobs_stops_at_old_job(self, mock_urlopen):
        """list_jobs stops pagination when it encounters a job older than JOB_LOOKBACK_SECS."""
        import app
        now_ms = int(time.time() * 1000)
        old_ms = int((time.time() - app.JOB_LOOKBACK_SECS - 3600) * 1000)  # definitely old
        login_resp = _login_response()
        page1_resp = MagicMock()
        page1_resp.read.return_value = json.dumps({
            "jobs": [
                {"id": "1", "user": "alice", "state": "RUNNING",
                 "isComplete": False, "startTime": now_ms},
                {"id": "2", "user": "bob", "state": "RUNNING",
                 "isComplete": False, "startTime": old_ms},  # triggers stop
            ],
            "next": "/jobs/?offset=100&limit=100",
        }).encode()
        mock_urlopen.side_effect = [login_resp, page1_resp]

        jobs = self.client.list_jobs()
        # Only alice (before the old-job cutoff) should be returned; no page 2 fetched
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["user"], "alice")
        self.assertEqual(mock_urlopen.call_count, 2)  # login + page 1 only

    @patch("app.urlopen")
    def test_list_jobs_stops_at_page_cap(self, mock_urlopen):
        """list_jobs stops after MAX_JOB_PAGES even if there are more pages."""
        import app
        now_ms = int(time.time() * 1000)

        def _make_page(job_id, has_next):
            m = MagicMock()
            m.read.return_value = json.dumps({
                "jobs": [{"id": str(job_id), "user": "u", "state": "RUNNING",
                          "isComplete": False, "startTime": now_ms}],
                "next": "/jobs/?offset=100&limit=100" if has_next else None,
            }).encode()
            return m

        # Login + MAX_JOB_PAGES pages all with next links
        responses = [_login_response()]
        for i in range(app.MAX_JOB_PAGES + 5):
            responses.append(_make_page(i, has_next=True))
        mock_urlopen.side_effect = responses

        jobs = self.client.list_jobs()
        # Should fetch exactly MAX_JOB_PAGES pages (each with 1 job)
        self.assertEqual(len(jobs), app.MAX_JOB_PAGES)
        # login call + MAX_JOB_PAGES page calls
        self.assertEqual(mock_urlopen.call_count, 1 + app.MAX_JOB_PAGES)


class TestMetricsSnapshot(unittest.TestCase):
    """Tests for MetricsSnapshot dataclass."""

    def test_to_dict(self):
        """Test metrics snapshot conversion to dict."""
        from app import MetricsSnapshot

        snap = MetricsSnapshot(
            active_user_jobs=10,
            active_small_jobs=5,
            active_large_jobs=5,
            active_reflection_jobs=2,
            registered_executors=3,
            executor_desired_small=1,
            executor_desired_large=2,
        )

        result = snap.to_dict()
        self.assertEqual(result["active_user_jobs"], 10)
        self.assertEqual(result["executor_desired_small"], 1)
        self.assertEqual(result["executor_desired_large"], 2)


class TestDremioMetricsCollector(unittest.TestCase):
    """Tests for DremioMetricsCollector class."""

    @patch("app.DremioClient")
    @patch("app.DremioLivenessClient")
    @patch("app.K8sStateCollector")
    def test_collect_jobs(self, mock_k8s, mock_liveness, mock_dremio):
        """Test job collection."""
        from app import DremioMetricsCollector

        mock_dremio_instance = MagicMock()
        # list_jobs() already returns only non-terminal jobs (terminal filtering done inside it)
        mock_dremio_instance.list_jobs.return_value = [
            {"user": "alice", "state": "RUNNING", "isComplete": False},
            {"user": "bob", "state": "QUEUED", "isComplete": False},
            {"user": "$dremio$", "state": "RUNNING", "isComplete": False},
        ]

        mock_liveness_instance = MagicMock()
        mock_liveness_instance.get_desired.return_value = (1, 2)

        mock_k8s_instance = MagicMock()
        mock_k8s_instance.get_replicas.return_value = 1

        mock_dremio.return_value = mock_dremio_instance
        mock_liveness.return_value = mock_liveness_instance
        mock_k8s.return_value = mock_k8s_instance

        collector = DremioMetricsCollector()

        # Set up the collectors on the instance
        collector._dremio = mock_dremio_instance
        collector._liveness = mock_liveness_instance
        collector._k8s = mock_k8s_instance

        result = collector._collect()

        self.assertEqual(result.active_user_jobs, 2)
        self.assertEqual(result.active_reflection_jobs, 1)
        # registered_executors = sum of K8s StatefulSet replicas (small=1 + large=1)
        self.assertEqual(result.registered_executors, 2)
        self.assertEqual(result.executor_desired_small, 1)

    @patch("app.DremioClient")
    @patch("app.DremioLivenessClient")
    @patch("app.K8sStateCollector")
    def test_terminal_jobs_filtered(self, mock_k8s, mock_liveness, mock_dremio):
        """Terminal jobs (COMPLETED/FAILED/CANCELED) must not count as active."""
        from app import DremioMetricsCollector

        mock_dremio_instance = MagicMock()
        # list_jobs() filters terminal jobs internally; it returns an empty list here
        mock_dremio_instance.list_jobs.return_value = []
        mock_liveness.return_value.get_desired.return_value = (0, 0)
        mock_k8s.return_value.get_replicas.return_value = 0
        mock_dremio.return_value = mock_dremio_instance

        collector = DremioMetricsCollector()
        collector._dremio = mock_dremio_instance
        collector._liveness = mock_liveness.return_value
        collector._k8s = mock_k8s.return_value

        result = collector._collect()

        self.assertEqual(result.active_user_jobs, 0)
        self.assertEqual(result.active_reflection_jobs, 0)

    @patch("app.DremioClient")
    @patch("app.DremioLivenessClient")
    @patch("app.K8sStateCollector")
    def test_collect_fails_open_on_exception(self, mock_k8s, mock_liveness, mock_dremio):
        """Any exception from list_jobs must fail open (active=99), not fail closed (active=0).

        Failing closed would cause the scale-gate to think there are no active jobs
        and allow KEDA to scale executors to zero, killing in-flight queries.
        """
        from app import DremioMetricsCollector

        mock_dremio_instance = MagicMock()
        mock_dremio_instance.list_jobs.side_effect = ConnectionError("connection refused")
        mock_liveness.return_value.get_desired.return_value = (1, 1)
        mock_k8s.return_value.get_replicas.return_value = 2
        mock_dremio.return_value = mock_dremio_instance

        collector = DremioMetricsCollector()
        collector._dremio = mock_dremio_instance
        collector._liveness = mock_liveness.return_value
        collector._k8s = mock_k8s.return_value

        result = collector._collect()

        # Must fail open — NEVER return 0 on error
        self.assertEqual(result.active_user_jobs, 99)
        self.assertEqual(result.active_small_jobs, 99)
        self.assertEqual(result.active_large_jobs, 99)
        # Scale-gate must hold executors at current count, not zero
        self.assertGreater(result.executor_desired_small, 0)
        self.assertGreater(result.executor_desired_large, 0)

    def test_k8s_state_collector_uses_statefulset(self):
        """K8sStateCollector must call read_namespaced_stateful_set, not deployment."""
        from app import K8sStateCollector
        collector = K8sStateCollector()
        if not collector._client_available:
            return  # Skip in environments without k8s
        mock_apps = MagicMock()
        mock_sts = MagicMock()
        mock_sts.spec.replicas = 2
        mock_apps.read_namespaced_stateful_set.return_value = mock_sts
        collector._apps = mock_apps

        count = collector.get_replicas("dremio-executor-small")

        self.assertEqual(count, 2)
        mock_apps.read_namespaced_stateful_set.assert_called_once_with(
            "dremio-executor-small", collector._namespace
        )


class TestDrainGuard(unittest.TestCase):
    """Tests for terminal drain guard logic."""

    def _make_collector(self):
        from app import DremioMetricsCollector
        with patch("app.DremioClient"), patch("app.DremioLivenessClient"), patch("app.K8sStateCollector"):
            c = DremioMetricsCollector()
        return c

    def test_drain_guard_holds_before_drain_complete(self):
        """After grace period, desired stays at current for TERMINAL_DRAIN_SECS before going to 0."""
        c = self._make_collector()
        c._last_active_small = 0.0  # long ago (infinite idle)

        result = c._compute_desired_small(current=2, small_jobs=0, reflection_jobs=0, dremio_desired=0)
        # First call: drain period starts, hold at 2
        self.assertEqual(result, 2)
        self.assertGreater(c._drain_started_small, 0.0)

    def test_drain_guard_returns_zero_after_drain_complete(self):
        """After TERMINAL_DRAIN_SECS, desired goes to 0."""
        import app
        c = self._make_collector()
        c._last_active_small = 0.0
        c._drain_started_small = time.time() - app.TERMINAL_DRAIN_SECS - 1  # drain elapsed

        result = c._compute_desired_small(current=2, small_jobs=0, reflection_jobs=0, dremio_desired=0)
        self.assertEqual(result, 0)
        self.assertEqual(c._drain_started_small, 0.0)  # reset

    def test_drain_guard_resets_when_jobs_become_active(self):
        """If jobs appear during drain period, drain timer resets."""
        c = self._make_collector()
        c._last_active_small = 0.0
        c._drain_started_small = time.time() - 60  # drain in progress

        result = c._compute_desired_small(current=2, small_jobs=1, reflection_jobs=0, dremio_desired=0)
        self.assertGreaterEqual(result, 1)
        self.assertEqual(c._drain_started_small, 0.0)  # reset

    def test_large_drain_guard_holds_before_drain_complete(self):
        """Large tier drain guard mirrors small tier behavior."""
        c = self._make_collector()
        c._last_active_large = 0.0

        result = c._compute_desired_large(current=3, large_jobs=0, dremio_desired=0)
        self.assertEqual(result, 3)
        self.assertGreater(c._drain_started_large, 0.0)

    def test_large_drain_guard_resets_on_active_jobs(self):
        """Large tier drain timer resets when large jobs appear."""
        c = self._make_collector()
        c._last_active_large = 0.0
        c._drain_started_large = time.time() - 60

        result = c._compute_desired_large(current=3, large_jobs=2, dremio_desired=0)
        self.assertGreaterEqual(result, 1)
        self.assertEqual(c._drain_started_large, 0.0)

    def test_within_grace_period_no_drain(self):
        """Within SCALE_DOWN_GRACE_SECS, drain guard does not activate."""
        c = self._make_collector()
        c._last_active_small = time.time() - 10  # 10s ago, within grace

        result = c._compute_desired_small(current=1, small_jobs=0, reflection_jobs=0, dremio_desired=0)
        self.assertEqual(result, 1)
        self.assertEqual(c._drain_started_small, 0.0)  # drain not started


if __name__ == "__main__":
    unittest.main()
