"""Tests for the Dremio metrics exporter."""

import json
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
    def test_list_jobs_with_next(self, mock_urlopen):
        """Test job list with pagination."""
        login_resp = _login_response()
        jobs_resp = MagicMock()
        jobs_resp.read.return_value = json.dumps({
            "jobs": [{"id": "1", "user": "alice"}],
            "next": "/jobs/?offset=100",
        }).encode()
        mock_urlopen.side_effect = [login_resp, jobs_resp]

        jobs = self.client.list_jobs()
        self.assertEqual(len(jobs), 1)

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


class TestDremioMetricsCollector(unittest.TestCase):
    """Tests for DremioMetricsCollector class."""

    @patch("app.DremioClient")
    @patch("app.DremioLivenessClient")
    @patch("app.K8sStateCollector")
    def test_collect_jobs(self, mock_k8s, mock_liveness, mock_dremio):
        """Test job collection."""
        from app import DremioMetricsCollector

        mock_dremio_instance = MagicMock()
        mock_dremio_instance.list_jobs.return_value = [
            {"user": "alice", "state": "RUNNING"},
            {"user": "bob", "state": "QUEUED"},
            {"user": "$dremio$", "state": "COMPLETED"},
        ]
        mock_dremio_instance.count_nodes.return_value = 3

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
        self.assertEqual(result.registered_executors, 3)
        self.assertEqual(result.executor_desired_small, 1)


if __name__ == "__main__":
    unittest.main()
