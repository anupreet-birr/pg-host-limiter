import unittest
from unittest.mock import Mock

from pg_host_limiter import Lease, PostgresHostLimiter
from pg_host_limiter.limiter import _host


class UnitTests(unittest.TestCase):
    def test_normalized_hosts_share_identity(self):
        for value, expected in [
            (" Example.COM. ", "example.com"),
            ("bücher.de", "xn--bcher-kva.de"),
            ("2001:0db8::1", "2001:db8::1"),
        ]:
            self.assertEqual(_host(value), expected)

    def test_urls_and_ports_are_not_hostnames(self):
        for value in [
            "",
            "https://example.com",
            "example.com:80",
            "a/b",
            "a b",
            "-a",
            "a..b",
        ]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                _host(value)

    def test_reject_invalid_settings_before_connecting(self):
        factory = Mock()
        for kwargs in [
            {"schema": "x; DROP SCHEMA public"},
            {"table": "x" * 64},
            {"acquire_timeout": float("nan")},
            {"poll_interval": 0},
            {"statement_timeout_ms": True},
        ]:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                PostgresHostLimiter(factory, **kwargs)
        limiter = PostgresHostLimiter(factory)
        for pause in [(float("inf"), 2), (0, float("nan")), (-1, 2), (3, 2)]:
            with self.subTest(pause=pause), self.assertRaises(ValueError):
                limiter.acquire("example.com", pause)
        factory.assert_not_called()

    def test_stable_namespaced_keys(self):
        one = PostgresHostLimiter(Mock())
        self.assertEqual(
            one._key("example.com"), PostgresHostLimiter(Mock())._key("example.com")
        )
        self.assertNotEqual(one._key("example.com"), one._key("other.com"))
        self.assertNotEqual(
            one._key("example.com"),
            PostgresHostLimiter(Mock(), table="other")._key("example.com"),
        )

    def test_completion_is_written_before_unlock_and_release_is_idempotent(self):
        conn = Mock()
        limiter = PostgresHostLimiter(Mock())
        lease = Lease(conn, limiter._table, "example.com", 123)
        lease.release()
        lease.release()
        calls = conn.execute.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertIn("UPDATE", calls[0].args[0].as_string())
        self.assertIn("pg_advisory_unlock", calls[1].args[0])
        conn.close.assert_called_once()
        with self.assertRaises(RuntimeError):
            lease.__enter__()

    def test_failed_completion_closes_session_without_explicit_unlock(self):
        conn = Mock()
        conn.execute.side_effect = RuntimeError("write failed")
        with self.assertRaises(RuntimeError):
            Lease(conn, PostgresHostLimiter(Mock())._table, "example.com", 1).release()
        conn.close.assert_called_once()
        self.assertEqual(conn.execute.call_count, 1)

    def test_cleanup_does_not_hide_original_request_error(self):
        conn = Mock()
        conn.execute.side_effect = RuntimeError("write failed")
        with self.assertRaisesRegex(ValueError, "original") as caught:
            with Lease(conn, PostgresHostLimiter(Mock())._table, "example.com", 1):
                raise ValueError("original")
        self.assertIn("cleanup", caught.exception.__notes__[0])


if __name__ == "__main__":
    unittest.main()
