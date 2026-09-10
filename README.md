# pg-host-limiter

**Coordinate per-host HTTP requests across Python workers using PostgreSQL.**

If your workers already share PostgreSQL, they can share a host cooldown too.
One worker holds a host lease while downloading; the next waits for that
request to finish and for a configurable pause. Different hosts run independently.
No Redis service or HTTP-client integration required.

Early release: synchronous Python 3.11+, psycopg 3, PostgreSQL session advisory
locks. This is cooperative request coordination, not a security rate limiter.

## Install

```sh
pip install 'pg-host-limiter @ git+https://github.com/anupreet-birr/pg-host-limiter.git@v0.1.0'
# If libpq is unavailable on your machine:
pip install 'psycopg[binary]>=3.2,<4'
```

Not published to PyPI yet. GitHub source and release distributions are available.

## Quick start

```python
import os
import psycopg
from pg_host_limiter import PostgresHostLimiter

limiter = PostgresHostLimiter(
    lambda: psycopg.connect(
        os.environ["PGHOSTLIMITER_DSN"], autocommit=True, connect_timeout=5
    ),
    schema="request_limits",
    acquire_timeout=30,
)

# Run once as an operator with schema/table creation permission.
limiter.ensure_schema()

# In each worker, hold the lease through response consumption AND close.
with limiter.acquire("example.com", pause_seconds=(1.0, 2.0)):
    download_and_close_response()  # your HTTP client, with its own timeout
```

All workers must use the same database, schema, table and pause policy.
The first request starts immediately. Later requests wait a random 1–2 seconds
after the previous recorded completion in this example. The pause is not an
HTTP timeout. Pass hostnames, not full URLs; use `urlsplit(url).hostname`.
Follow redirects manually, closing the response and acquiring the destination
host's lease for each hop. Never acquire the same host recursively.

## How it works

1. Open a fresh, owned autocommit database connection.
2. Poll a namespaced PostgreSQL session advisory lock for the hostname.
3. Read committed timing state and wait out the cooldown while holding the lock.
4. Record the start using PostgreSQL `clock_timestamp()` and return the lease.
5. On release, commit completion **before** unlocking, then close the connection.

`AcquireTimeout` means the lock/cooldown wait could not complete in time.
There are no automatic retries. Database errors fail acquisition rather than
silently permitting a request. Release is idempotent. Prefer `with`; a forgotten
lease holds both a connection and a lock until the session closes.

## Operational boundaries

- **Not fencing:** if a database session dies while its HTTP request continues,
  PostgreSQL can release its lock and another worker can start. No strict
  external concurrency guarantee across network partitions.
- After a worker crash, cooldown uses the recorded start if completion is
  unknown. It cannot reconstruct when the remote request actually finished.
- Use direct PostgreSQL connections. Transaction/statement pooling (including
  those PgBouncer modes) is incompatible with session locks. The factory must
  return a new connection owned exclusively by this acquisition, not a borrowed
  pool connection or an existing transaction.
- Every waiting or active acquisition consumes a connection. Bound worker
  counts to your connection budget. No fairness guarantee or priority queue.
- `acquire_timeout` starts after connection setup; individual SQL operations
  can extend elapsed time by `statement_timeout_ms` (default 5,000 ms).
  Set database connection and HTTP timeouts separately.
- Host normalization handles case, IDNA, trailing dots and IP address spelling;
  it does not resolve DNS aliases or merge subdomains. Ports share a host limit.
- Maintain synchronized/stable database time. Clock adjustments affect cooldowns.
- Respect site policies, robots rules where applicable, and API contracts.
  This package does not bypass upstream access controls.

## Least-privilege setup

Run `ensure_schema()` with a setup connection, then give the worker role only
`CONNECT` to the database, `USAGE` on the schema, and `SELECT, INSERT, UPDATE`
on the limiter's table. It does not need DELETE, sequence, schema creation or
superuser privileges. Standard PostgreSQL advisory-lock functions must be
available. Keep this table separate from business data. Worker code must not
call `ensure_schema()` after switching to its restricted role.

```sql
-- Adapt these identifiers to your database and pre-created worker role.
GRANT CONNECT ON DATABASE app_database TO request_worker;
GRANT USAGE ON SCHEMA request_limits TO request_worker;
GRANT SELECT, INSERT, UPDATE
    ON request_limits.host_request_state TO request_worker;
```

Rows persist per hostname; there is no automatic cleanup. This is a pacing
utility, not an audit log or a requests-per-second token bucket.

## Development and tests

```sh
pip install -e . 'psycopg[binary]>=3.2,<4'
python -m unittest discover -s tests -p test_unit.py -v
# IMPORTANT: use a disposable database; integration tests create/drop schemas
# and a temporary role. The test connection must have setup privileges.
export PGHOSTLIMITER_TEST_DSN='postgresql://postgres:test_only@localhost:5432/limiter_test'
python -m unittest discover -s tests -v
```

Integration tests skip if the test DSN is absent. CI supplies a disposable
PostgreSQL service and runs both suites. See [design](docs/design.md),
[contribution guide](CONTRIBUTING.md) and [security policy](SECURITY.md).

## License

Apache-2.0. Copyright 2026 anupreet-birr. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
