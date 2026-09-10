# Contract for v0.1

Coordinate cooperating synchronous HTTP workers using a shared PostgreSQL
database, schema and table. At most one live database session holds a lease
for a normalized hostname. Different hosts proceed independently. Hold the
lease until the response body has been consumed or closed.

The next request waits a randomized minimum gap after the previous recorded
completion (or start if the previous worker crashed). Use database wall-clock
timestamps, and commit completion before unlocking. Session advisory locks
are not fencing: a broken database connection can release a lock while its
HTTP request is still running. This is not a hard external rate-limit guarantee.

Each acquisition owns a fresh autocommit connection. No application
transaction remains open during a download. SQL, connection and HTTP timeouts
are separate from the acquisition wait timeout. No network/provider retries.
No HTTP client, database provisioning, credentials or application dependencies
are bundled. Schema creation is an explicit operator operation.

Acceptance: host normalization and finite-value validation; stable lock keys;
same-host serialization and completion gaps across independent sessions;
different-host independence; acquisition timeout and exception cleanup;
crashed-session recovery; SQL identifier validation; runtime role without DDL
permission; deterministic release ordering and safe package contents.
