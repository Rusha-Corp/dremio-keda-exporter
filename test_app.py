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
    def test_list_jobs_success(self, mock_urlopen):
        """Test successful job listing."""
        login_resp = _login_response()
        jobs_resp = MagicMock()
        jobs_resp.read.return_value = json.dumps({
            "jobs": [
                {"id": "1", "user": "alice", "state": "RUNNING"},
                {"id": "2", "user": "$dremio$", "state": "COMPLETED"},
            ],
            "next": None,
        }).encode()
        mock_urlopen.side_effect = [login_resp, jobs_resp]

        jobs = self.client.list_jobs()
        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0]["user"], "alice")
        self.assertEqual(jobs[1]["user"], "$dremio$")

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
    def test_count_nodes_success(self, mock_urlopen):
        """Test successful node count."""
        login_resp = _login_response()
        submit_resp = MagicMock()
        submit_resp.read.return_value = json.dumps(
            {"id": "test-job-id"}
        ).encode()
        status_resp = MagicMock()
        status_resp.read.return_value = json.dumps(
            {"jobState": "COMPLETED"}
        ).encode()
        results_resp = MagicMock()
        results_resp.read.return_value = json.dumps(
            {"rows": [{"cnt": 3}]}
        ).encode()
        mock_urlopen.side_effect = [
            login_resp, submit_resp, status_resp, results_resp
        ]

        count = self.client.count_nodes()
        self.assertEqual(count, 3)

    @patch("app.urlopen")
    def test_count_nodes_failed(self, mock_urlopen):
        """Test node count when query fails."""
        login_resp = _login_response()
        submit_resp = MagicMock()
        submit_resp.read.return_value = json.dumps(
            {"id": "test-job-id"}
        ).encode()
        status_resp = MagicMock()
        status_resp.read.return_value = json.dumps(
            {"jobState": "FAILED"}
        ).encode()
        mock_urlopen.side_effect = [login_resp, submit_resp, status_resp]

        count = self.client.count_nodes()
        self.assertEqual(count, 0)


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


class TestK8sStateCollector(unittest.TestCase):
    """Tests for K8sStateCollector.get_statefulset_info returning 4-tuple."""

    def test_get_statefulset_info_returns_count(self):
        """get_statefulset_info must return (spec, ready, scale_ts, scale_count)."""
        from app import K8sStateCollector

        collector = K8sStateCollector()
        if not collector._available:
            self.skipTest("K8s client not available in test environment")

        mock_sts = MagicMock()
        mock_sts.spec.replicas = 2
        mock_sts.status.ready_replicas = 1
        mock_sts.metadata.annotations = {
            "dremio.io/scale-requested-at": str(int(time.time() * 1000)),
            "dremio.io/scale-requested-count": "3",
        }
        collector._apps = MagicMock()
        collector._apps.read_namespaced_stateful_set.return_value = mock_sts

        spec, ready, ts, count = collector.get_statefulset_info("dremio-executor-large")
        self.assertEqual(spec, 2)
        self.assertEqual(ready, 1)
        self.assertGreater(ts, 0)
        self.assertEqual(count, 3)

    def test_get_statefulset_info_missing_count(self):
        """Missing scale-requested-count annotation should return 0 for count."""
        from app import K8sStateCollector

        collector = K8sStateCollector()
        if not collector._available:
            self.skipTest("K8s client not available in test environment")

        mock_sts = MagicMock()
        mock_sts.spec.replicas = 1
        mock_sts.status.ready_replicas = 0
        mock_sts.metadata.annotations = {
            "dremio.io/scale-requested-at": str(int(time.time() * 1000)),
        }
        collector._apps = MagicMock()
        collector._apps.read_namespaced_stateful_set.return_value = mock_sts

        spec, ready, ts, count = collector.get_statefulset_info("dremio-executor-large")
        self.assertEqual(count, 0)


class TestSignal3Propagation(unittest.TestCase):
    """Tests that Signal 3 propagates scale-requested-count to KEDA."""

    def _make_collector(self):
        """Create a collector with mocked dependencies."""
        from app import DremioMetricsCollector
        collector = DremioMetricsCollector()
        collector._dremio = MagicMock()
        collector._k8s = MagicMock()
        return collector

    @patch("app.time")
    def test_signal3_propagates_count(self, mock_time):
        """When annotation is fresh and count=3, desired_large should be 3."""
        mock_time.time.return_value = 1000.0
        mock_time.sleep = MagicMock()

        from app import DremioMetricsCollector, SCALE_DOWN_GRACE_SECS

        collector = self._make_collector()

        # No recent jobs
        collector._dremio.list_jobs.return_value = []
        collector._dremio.count_nodes.return_value = 0

        # Annotation was written 10s ago, count=3
        fresh_ts = 1000.0 - 10  # 10 seconds ago
        collector._k8s.get_statefulset_info.side_effect = [
            # small tier: spec=1, ready=1, ts=0, count=0
            (1, 1, 0.0, 0),
            # large tier: spec=0, ready=0, ts=fresh, count=3
            (0, 0, fresh_ts, 3),
        ]

        result = collector._collect()

        # Signal 3 should propagate count=3 → spec_large = max(0, 3) = 3
        # Within grace → desired = spec_replicas = 3
        self.assertEqual(result.executor_desired_large, 3)

    @patch("app.time")
    def test_signal3_expired_does_not_propagate(self, mock_time):
        """When annotation is older than grace, count should not affect desired."""
        mock_time.time.return_value = 1000.0
        mock_time.sleep = MagicMock()

        from app import DremioMetricsCollector, SCALE_DOWN_GRACE_SECS

        collector = self._make_collector()

        collector._dremio.list_jobs.return_value = []
        collector._dremio.count_nodes.return_value = 0

        # Annotation was written SCALE_DOWN_GRACE_SECS + 100s ago (expired)
        expired_ts = 1000.0 - SCALE_DOWN_GRACE_SECS - 100
        collector._k8s.get_statefulset_info.side_effect = [
            (1, 1, 0.0, 0),
            (0, 0, expired_ts, 3),
        ]

        # No grace timers set yet (first collect)
        result = collector._collect()

        # Annotation expired → signal doesn't fire → desired = 0
        self.assertEqual(result.executor_desired_large, 0)

    @patch("app.time")
    def test_signal3_count_higher_than_spec(self, mock_time):
        """When annotation count > spec_replicas, spec should be bumped up."""
        mock_time.time.return_value = 1000.0
        mock_time.sleep = MagicMock()

        collector = self._make_collector()

        collector._dremio.list_jobs.return_value = []
        collector._dremio.count_nodes.return_value = 0

        fresh_ts = 1000.0 - 10
        collector._k8s.get_statefulset_info.side_effect = [
            (1, 1, 0.0, 0),
            # spec=1 but annotation count=5 → max(1,5) = 5
            (1, 1, fresh_ts, 5),
        ]

        result = collector._collect()

        self.assertEqual(result.executor_desired_large, 5)

    @patch("app.time")
    def test_signal3_count_lower_than_spec(self, mock_time):
        """When annotation count < spec_replicas, spec should not be reduced."""
        mock_time.time.return_value = 1000.0
        mock_time.sleep = MagicMock()

        collector = self._make_collector()

        collector._dremio.list_jobs.return_value = []
        collector._dremio.count_nodes.return_value = 0

        fresh_ts = 1000.0 - 10
        collector._k8s.get_statefulset_info.side_effect = [
            (1, 1, 0.0, 0),
            # spec=4, annotation count=2 → max(4,2) = 4 (spec wins)
            (4, 3, fresh_ts, 2),
        ]

        result = collector._collect()

        # spec_replicas=4 is already higher, annotation count=2 doesn't reduce it
        self.assertEqual(result.executor_desired_large, 4)


if __name__ == "__main__":
    unittest.main()
