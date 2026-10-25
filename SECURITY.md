# Security policy

flink-mcp-server is a community project, not an official Apache Flink project.

## Reporting a vulnerability

Please report problems privately through GitHub's
[security advisories](https://github.com/hakkiburakalks/flink-mcp-server/security/advisories/new)
rather than a public issue. Include the version (`flink-mcp-server --version`), how it is deployed
(stdio or HTTP) and the smallest steps that reproduce it. Do not include real tokens or logs
from systems you do not own.

This is a volunteer-maintained project; there is no guaranteed response time.

## Supported versions

Only the latest release on PyPI receives fixes.

## Scope and design

See [docs/security.md](docs/security.md) for the security model, the resource limits and the
known limits (raw PromQL tools are not scoped by `cluster`; logs can contain secrets; the HTTP
transport is plain HTTP).
