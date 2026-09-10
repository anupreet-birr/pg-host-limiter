"""Integration tests: PGHOSTLIMITER_TEST_DSN MUST reference a disposable database."""

import os
import subprocess
import sys
import threading
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor

import psycopg
from psycopg import sql

from pg_host_limiter import AcquireTimeout, PostgresHostLimiter

DSN = os.environ.get("PGHOSTLIMITER_TEST_DSN")


@unittest.skipUnless(
    DSN, "Set PGHOSTLIMITER_TEST_DSN to a disposable PostgreSQL database"
)
class PostgresTests(unittest.TestCase):
    def setUp(self):
        self.schema = "test_" + uuid.uuid4().hex
        self.factory = lambda: psycopg.connect(DSN, autocommit=True, connect_timeout=5)
        self.limiter = PostgresHostLimiter(self.factory, schema=self.schema)
        self.limiter.ensure_schema()

    def tearDown(self):
        with self.factory() as conn:
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema))
            )

    def test_same_host_serializes_and_gap_starts_after_completion(self):
        barrier = threading.Barrier(4)
        intervals = []

        def worker():
            barrier.wait()
            with self.limiter.acquire("Example.COM.", (0.08, 0.08)):
                start = time.monotonic()
                time.sleep(0.08)
                end = time.monotonic()
            intervals.append((start, end))

        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: worker(), range(4)))
        intervals.sort()
        for previous, current in zip(intervals, intervals[1:]):
            self.assertGreaterEqual(current[0] - previous[1], 0.075)

    def test_different_hosts_are_independent(self):
        with self.limiter.acquire("one.example"):
            other = PostgresHostLimiter(
                self.factory, schema=self.schema, acquire_timeout=0.3
            )
            with other.acquire("two.example"):
                pass

    def test_timeout_does_not_leak_lock(self):
        fast = PostgresHostLimiter(
            self.factory, schema=self.schema, acquire_timeout=0.1
        )
        with self.limiter.acquire("example.com"):
            with self.assertRaises(AcquireTimeout):
                fast.acquire("example.com")
        with fast.acquire("example.com"):
            pass

    def test_exception_releases_and_completion_timestamp_is_fresh(self):
        with self.factory() as conn:
            before = conn.execute("SELECT clock_timestamp()").fetchone()[0]
        with self.assertRaises(ValueError):
            with self.limiter.acquire("example.com"):
                time.sleep(0.05)
                raise ValueError("request failure")
        with self.factory() as conn:
            start, end = conn.execute(
                sql.SQL(
                    "SELECT started_at, completed_at FROM {}.host_request_state"
                ).format(sql.Identifier(self.schema))
            ).fetchone()
        self.assertGreater(start, before)
        self.assertGreater((end - start).total_seconds(), 0.04)
        with self.limiter.acquire("example.com"):
            pass

    def test_cooldown_timeout_releases_session(self):
        with self.limiter.acquire("example.com"):
            pass
        fast = PostgresHostLimiter(
            self.factory, schema=self.schema, acquire_timeout=0.1
        )
        with self.assertRaises(AcquireTimeout):
            fast.acquire("example.com", (1, 1))
        with fast.acquire("example.com"):
            pass

    def test_worker_crash_releases_session_lock(self):
        program = """
import os, psycopg
from pg_host_limiter import PostgresHostLimiter
limiter = PostgresHostLimiter(lambda: psycopg.connect(os.environ['PGHOSTLIMITER_TEST_DSN'], autocommit=True), schema=os.environ['TEST_SCHEMA'])
lease = limiter.acquire('crash.example')
os._exit(0)
"""
        subprocess.run(
            [sys.executable, "-c", program],
            check=True,
            timeout=10,
            env={**os.environ, "TEST_SCHEMA": self.schema},
        )
        with self.limiter.acquire("crash.example", (0.05, 0.05)):
            pass

    def test_transaction_connections_are_rejected(self):
        limiter = PostgresHostLimiter(lambda: psycopg.connect(DSN), schema=self.schema)
        with self.assertRaisesRegex(ValueError, "autocommit"):
            limiter.acquire("example.com")

    def test_missing_schema_fails_closed(self):
        missing = PostgresHostLimiter(
            self.factory, schema="missing_" + uuid.uuid4().hex
        )
        with self.assertRaises(psycopg.errors.UndefinedTable):
            missing.acquire("example.com")

    def test_runtime_role_needs_no_ddl_or_delete(self):
        role = "role_" + uuid.uuid4().hex
        with self.factory() as conn:
            conn.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
            conn.execute(
                sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                    sql.Identifier(self.schema), sql.Identifier(role)
                )
            )
            conn.execute(
                sql.SQL(
                    "GRANT SELECT, INSERT, UPDATE ON {}.host_request_state TO {}"
                ).format(sql.Identifier(self.schema), sql.Identifier(role))
            )

        def runtime_factory():
            conn = self.factory()
            conn.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(role)))
            return conn

        try:
            with PostgresHostLimiter(runtime_factory, schema=self.schema).acquire(
                "example.com"
            ):
                pass
            with runtime_factory() as conn:
                with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                    conn.execute(
                        sql.SQL("DELETE FROM {}.host_request_state").format(
                            sql.Identifier(self.schema)
                        )
                    )
        finally:
            with self.factory() as conn:
                conn.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
                conn.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


if __name__ == "__main__":
    unittest.main()
