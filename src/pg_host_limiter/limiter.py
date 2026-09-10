"""Session locks serialize hosts; committed timestamps coordinate cooldowns."""

from __future__ import annotations

import hashlib
import ipaddress
import math
import random
import re
import time
from collections.abc import Callable
from types import TracebackType

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus


class AcquireTimeout(TimeoutError):
    """The lock/cooldown wait exceeded the configured acquisition timeout."""


def _host(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("Pass a hostname, not a URL or port.")
    value = value.strip().rstrip(".").lower()
    try:
        return str(ipaddress.ip_address(value))
    except ValueError:
        pass
    try:
        value = value.encode("idna").decode("ascii")
    except UnicodeError as exc:
        raise ValueError("Invalid hostname.") from exc
    if len(value) > 253 or not all(
        re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", part)
        for part in value.split(".")
    ):
        raise ValueError("Pass a hostname, not a URL or port.")
    return value


def _finite(value: float, name: str, *, positive: bool = False) -> float:
    value = float(value)
    if not math.isfinite(value) or value < 0 or (positive and value == 0):
        raise ValueError(
            f"{name} must be finite and {'positive' if positive else 'nonnegative'}."
        )
    return value


class Lease:
    """An owned database session. Always release it; prefer a with statement."""

    def __init__(
        self,
        connection: psycopg.Connection,
        table: sql.Composed,
        hostname: str,
        key: int,
    ):
        self._connection = connection
        self._table = table
        self._hostname = hostname
        self._key = key
        self._released = False

    def release(self) -> None:
        """Commit completion before unlocking, then close; safe to call twice."""
        if self._released:
            return
        self._released = True
        try:
            self._connection.execute(
                sql.SQL(
                    "UPDATE {} SET completed_at = clock_timestamp() WHERE hostname = %s"
                ).format(self._table),
                (self._hostname,),
            )
            self._connection.execute("SELECT pg_advisory_unlock(%s)", (self._key,))
        finally:
            self._connection.close()

    def __enter__(self) -> Lease:
        if self._released:
            raise RuntimeError("A released lease cannot be reused.")
        return self

    def __exit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        try:
            self.release()
        except Exception as cleanup_error:
            if error is None:
                raise
            error.add_note(f"Lease cleanup also failed: {type(cleanup_error).__name__}")


class PostgresHostLimiter:
    """One request per hostname plus a completion-to-start randomized pause.

    The factory must return a fresh, owned, autocommit psycopg connection.
    Connection establishment is outside acquire_timeout; configure connect_timeout
    in the factory. Each SQL statement is bounded separately.
    """

    def __init__(
        self,
        connection_factory: Callable[[], psycopg.Connection],
        *,
        schema: str = "public",
        table: str = "host_request_state",
        acquire_timeout: float = 30,
        poll_interval: float = 0.05,
        statement_timeout_ms: int = 5000,
    ):
        for identifier in (schema, table):
            if not isinstance(identifier, str) or not re.fullmatch(
                r"[A-Za-z_][A-Za-z0-9_]{0,62}", identifier
            ):
                raise ValueError(
                    "Schema/table must be simple SQL identifiers (up to 63 characters)."
                )
        self._factory = connection_factory
        self._schema = schema
        self._name = table
        self._table = sql.SQL("{}.{}").format(
            sql.Identifier(schema), sql.Identifier(table)
        )
        self._timeout = _finite(acquire_timeout, "acquire_timeout", positive=True)
        self._poll = _finite(poll_interval, "poll_interval", positive=True)
        if (
            type(statement_timeout_ms) is not int
            or not 1 <= statement_timeout_ms <= 2147483647
        ):
            raise ValueError(
                "statement_timeout_ms must be a positive PostgreSQL integer."
            )
        self._statement_timeout = statement_timeout_ms

    def _connect(self) -> psycopg.Connection:
        connection = self._factory()
        try:
            if (
                connection.closed
                or not connection.autocommit
                or (connection.info.transaction_status != TransactionStatus.IDLE)
            ):
                raise ValueError(
                    "Connection factory must return a fresh idle autocommit connection."
                )
            connection.execute(
                "SELECT set_config('statement_timeout', %s, false)",
                (str(self._statement_timeout),),
            )
            return connection
        except BaseException:
            connection.close()
            raise

    def ensure_schema(self) -> None:
        """Explicit setup using a DDL-capable factory; not called by acquire."""
        connection = self._connect()
        try:
            connection.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(self._schema)
                )
            )
            connection.execute(
                sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    hostname TEXT PRIMARY KEY,
                    started_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ
                )
            """).format(self._table)
            )
        finally:
            connection.close()

    def _key(self, hostname: str) -> int:
        namespace = f"pg-host-limiter:v1:{self._schema}:{self._name}:{hostname}"
        return int.from_bytes(
            hashlib.sha256(namespace.encode()).digest()[:8], "big", signed=True
        )

    def acquire(
        self, hostname: str, pause_seconds: tuple[float, float] = (0, 0)
    ) -> Lease:
        """Wait for a lease; callers own it until release, including streaming."""
        hostname = _host(hostname)
        minimum, maximum = (_finite(value, "pause_seconds") for value in pause_seconds)
        if maximum < minimum:
            raise ValueError("Maximum pause must be at least the minimum pause.")
        gap = random.uniform(minimum, maximum)
        connection = self._connect()
        deadline = time.monotonic() + self._timeout
        key = self._key(hostname)
        try:
            while True:
                if time.monotonic() >= deadline:
                    raise AcquireTimeout("Timed out waiting for a host lease.")
                if connection.execute(
                    "SELECT pg_try_advisory_lock(%s)", (key,)
                ).fetchone()[0]:
                    break
                time.sleep(min(self._poll, max(0, deadline - time.monotonic())))
            row = connection.execute(
                sql.SQL("""
                SELECT GREATEST(started_at, completed_at), clock_timestamp()
                FROM {} WHERE hostname = %s
            """).format(self._table),
                (hostname,),
            ).fetchone()
            if row and row[0] is not None:
                wait = max(0, gap - (row[1] - row[0]).total_seconds())
                if time.monotonic() + wait >= deadline:
                    raise AcquireTimeout(
                        "Host cooldown exceeds the remaining acquisition time."
                    )
                time.sleep(wait)
            if time.monotonic() >= deadline:
                raise AcquireTimeout("Timed out waiting for a host lease.")
            connection.execute(
                sql.SQL("""
                INSERT INTO {} (hostname, started_at) VALUES (%s, clock_timestamp())
                ON CONFLICT (hostname) DO UPDATE SET started_at = EXCLUDED.started_at
            """).format(self._table),
                (hostname,),
            )
            return Lease(connection, self._table, hostname, key)
        except BaseException:
            connection.close()
            raise
