# Security policy

Do not post credentials or sensitive database details in public issues.
For vulnerabilities, use GitHub's private vulnerability reporting on this
repository's Security tab. If unavailable, open a non-sensitive issue asking
the maintainer for a private reporting channel before sharing details.

This package coordinates cooperating workers; it is not an authorization layer,
a distributed fencing mechanism, or protection against malicious clients.
Use least-privilege database roles and network controls. Version 0.1 is an early
release without a security audit or a support SLA.
