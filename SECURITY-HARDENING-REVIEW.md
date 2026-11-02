# Independent security hardening review

## Executive Summary

Reviewed the local v0.4.4 working copy on 2026-11-02. Confirmed resource and error-disclosure
weaknesses were addressed with isolated regression tests. Existing read-only tools, argument
contracts, filtering, cross-cluster PromQL defaults and authentication policy are preserved.
Recommendation: **CONDITIONALLY READY** for controlled deployments, subject to the limitations
below. This is not a claim of production certification or absence of vulnerabilities.

Release conditions: run the expanded CI platform/version matrix and isolated integration jobs;
review remaining mutable third-party Action references and validate the external PyPI Trusted
Publisher/environment configuration. No release operation was performed.

## Baseline

- HEAD: `7364eb1a77efa8131b7fa24669ed585ea26c9ec2`; source version `0.4.4`.
- Initial `git status --short`: empty. Existing ignored `.venv`, `dist`, caches and local assistant
  directories were present and were not overwritten. Git reports an inaccessible global ignore
  file under the sandbox; repository status and diff inspection remain available.
- Windows, PowerShell 5.1; uv 0.12.1; local virtual environment Python 3.11.15. Source requires
  Python >=3.10 and advertises 3.10–3.13. No runtime compatibility claim for newer Python versions.
- Installed editable distribution metadata still says 0.4.2; imports were verified against this
  repository's source, which says 0.4.4. Built artifacts have version 0.4.4.
- Direct installed dependencies: MCP 1.30.0, HTTPX 0.28.1, PyYAML 6.0.3. Relevant transitive
  dependencies: AnyIO 4.15.1, Starlette 1.10.0, Uvicorn 0.54.0, Pydantic 2.14.1,
  httpcore 1.0.9, h11 0.16.0. Tests: pytest 9.1.1, pytest-asyncio 1.4.0.
- `uv.lock` contains versioned artifact hashes; locked export succeeds without changing the lock.
  Library dependency ranges in `pyproject.toml` remain unchanged. No new runtime dependencies.
- Baseline unit run: **300 passed, 0 failed, 13 integration tests deselected**.
- Mock HTTP/ASGI and subprocess environments available. No live Flink, Prometheus or Kubernetes
  infrastructure used, discovered or provisioned. Existing integration fixtures were inspected.

## Findings

Locations below refer to the baseline commit, so they remain reproducible after edits.

| ID | Severity | Classification | Root cause, evidence and impact | Remediation / verification |
| --- | --- | --- | --- | --- |
| SEC-01 | High (availability) | CONFIRMED | `src/flink_mcp/client.py:141`: `aiter_lines()` buffers a full logical line before byte/deadline checks. An upstream unterminated line can grow memory without reaching the check; elapsed checks also cannot interrupt stalled reads. `client.py:137` reads error bodies without a cap. | Incremental byte processing, 64 KiB logical-line cap, 64 MiB total cap, bounded error body and cancellable 30 s deadline including slot wait. `test_hardening.py` covers oversized/unterminated lines, exact byte boundaries, CR/LF/CRLF, invalid UTF-8, stalls, cancellation and small/large partial streams; existing log tests cover normal logs and stack filtering. |
| SEC-02 | Medium | PARTIALLY CONFIRMED | `src/flink_mcp/prometheus.py:149,166`: concurrency already limited to 8; upstream timeout already supplied. HTTPX timeout is inactivity-based and slot waits lack an elapsed deadline. Repeated slow chunks can retain work. Response/series/point/query-length/range limits already exist but do not bound evaluation complexity. | Local elapsed deadline including queue wait; configurable concurrency; finite time validation; opt-in raw-query disable. Slow/queued requests close and release slots in regression tests. No PromQL syntax heuristic is presented as a cost guarantee. |
| SEC-03 | Informational | FALSE POSITIVE as an authorization bypass | `src/flink_mcp/server.py:377,392`, `config.py:368`: raw queries deliberately use the selected connection without applying its selector. Existing documentation explicitly states the behavior. No per-cluster authorization promise exists. | Defaults preserved. Global `allow_raw_queries: false` rejects both raw tools; generated diagnostics remain available. Tests send a cluster-b selector through cluster-a, verify unchanged query forwarding by default and zero upstream raw requests with policy disabled. Strict authorization requires upstream tenant-scoped credentials/endpoints. |
| SEC-04 | Informational | NOT REPRODUCED as an unauthenticated bypass | `src/flink_mcp/transport.py:36,88`: constant-time bearer comparison protects every HTTP path. Host/Origin checks remain on for loopback and unauthenticated binds. Authenticated non-loopback binds without an explicit allowlist accept any Host/Origin; a rebinding browser still lacks the bearer token. | Existing policy preserved; no evidence justified breaking authenticated LAN/proxy access. Existing negative Host/Origin/initialize tests and new every-path/query-token rejection tests pass. Require TLS proxy and explicit allowed hosts for defense in depth. Generated bearer output at `transport.py:65` is deliberate bootstrap behavior, not an accidental token exception. |
| SEC-05 | Low (defense in depth) | PARTIALLY CONFIRMED | `validation.py:23` uses regex `$` with `match`, accepting a final newline; `kube.py:319` trusts discovery names during URL assembly; `kube.py:327` uses an overbroad server string prefix for credential attachment. Fixed tool paths and `safe_path` already prevent the claimed tool-to-other-API path injection. | `fullmatch`, validation during REST URL assembly, and proxy-path prefix for credential attachment. Tests reject newline and path injections and ensure `https://k.evil/...` does not receive auth for `https://k`. No writable Kubernetes API action added. |
| SEC-06 | Medium | CONFIRMED | `config.py:73`, `kube.py:107`: YAML parser exceptions include source excerpts. `kube.py:209`: exec stderr is copied into errors. `client.py:96`, `prometheus.py:166`, `kube.py:271` (GET error path): request exception strings may include credential-bearing URLs. Upstream Flink/Kube error details could echo bearer credentials. | Withhold YAML source/stderr and connection exception text and their sensitive exception chains; redact known bearer credentials in error details, including kube-authenticated Flink requests. Synthetic secret and YAML/upstream-error regression tests pass. Arbitrary successful logs/configuration remain sensitive data. |
| SEC-07a | Medium (local installer) | CONFIRMED | `install.ps1:80`: interpolated `Url` is passed to `cmd.exe /c`; shell metacharacters become command syntax when `-ClaudeCode` is used. Requires control of installer input; not remotely reachable through MCP tools. | Argument-array invocation of Claude without building a shell command. Mocked PowerShell regression verifies a URL containing `&` arrives as one literal argument with the `--` separator. |
| SEC-07b | Low / supply-chain trust | PARTIALLY CONFIRMED | `install.sh:37`, `install.ps1:47`: mutable uv bootstrap; workflows use mutable Action refs and implicit token permissions. `publish.yml:22` embeds a release-tag expression directly in shell source (trusted release-author prerequisite). Runtime lockfile already carries artifact hashes; publishing already requires a release event and an OIDC-enabled job. | Exact package/bootstrap version options, download-before-execute in sh, locked CI install/export/audit, checkout SHA pinning, explicit read permissions, tag through environment variable, pinned check tools, Dependabot. Mutable refs for setup-uv, kind, artifact actions and publishing remain a publisher trust risk; no unverifiable SHAs invented. |
| REL-01 | Medium (local resource lifecycle) | CONFIRMED | `kube.py:203,205`: `communicate()` accumulates arbitrary plugin output; timeout kills without reaping; cancellation bypasses kill. A trusted-but-broken credential plugin can exhaust memory or survive a cancelled tool call. | 1 MiB cap per output stream; cancel reader tasks, kill direct child and drain/reap on timeout/cancellation. Regression tests exercise output overflow, cancellation and timeout on Windows. Descendant-process cleanup is not guaranteed. |
| REL-02 | Low (reliability) | CONFIRMED | `prometheus.py:258`: malformed successful result shapes raise raw KeyError/TypeError/AttributeError instead of PromError. Flink's error formatter also assumes an error string list. | Normalize malformed query results and guard Flink error-detail types. Malformed Prometheus shape regressions pass; supported result contracts retained. |

Reproduction commands use only fake upstreams and synthetic credentials:

```powershell
.venv/Scripts/python.exe -m pytest tests/test_hardening.py tests/test_security.py tests/test_transport.py tests/test_installers.py -q -p no:cacheprovider
.venv/Scripts/python.exe -m pytest -q -p no:cacheprovider -m "not integration and not k8s"
uvx bandit==1.9.4 -r src -q
uv export --locked --no-dev --no-emit-project --format requirements-txt --output-file audit-requirements.txt
uvx pip-audit==2.10.1 -r audit-requirements.txt --require-hashes --disable-pip
```

## Implementation

- `client.py`: bounded streaming, elapsed deadline, partial status and error redaction.
- `prometheus.py`, `config.py`, `server.py`: request deadlines, configurable concurrency/raw policy,
  numeric validation, malformed result normalization, sensitive exception cleanup.
- `kube.py`, `validation.py`: proxy/name checks, bounded credential plugin output and cleanup.
- `transport.py`: only a narrowly documented Bandit B104 suppression on wildcard comparison;
  authentication and DNS-rebinding configuration behavior unchanged.
- Installers: pinned installation options, safer sh bootstrap, shell-free Claude arguments.
- Workflows: 3 OS x 4 Python matrix, locked unit environment, vulnerability/static/package job,
  explicit permissions and safe release-tag handling. Publishing remains release-event-only,
  under `pypi` environment, with `id-token: write` confined to publishing. No PR publication path.
- `.github/dependabot.yml`: weekly uv and Actions updates. `.gitignore`: audit scratch exclusion.
- Tests: `test_hardening.py`, `test_installers.py`; existing kube test now expects withheld stderr.
- README and security guide: installation trust, limits, isolation and operational tradeoffs.

## Security Verification

Final security/transport/installer subset: **128 passed, 0 failed** before the final full-suite
run. This includes existing defenses rather than assuming they were absent. New hardening tests
exercise bounded log memory, partial data, cancellation, malformed responses, credential errors,
explicit raw-query policy, proxy path construction and subprocess resource cleanup.

Bandit 1.9.4: **0 findings** after one B104 suppression (comparison with wildcard bind literals,
not a socket bind). Bandit emits two redundant suppression warnings for other AST literals on
that line; no other tests disabled. pip-audit 2.10.1 reports **0 known vulnerabilities** for both
installed environment and the hash-bearing locked runtime export. Audits are dated observations,
not guarantees against unknown vulnerabilities. No dependency upgrades were warranted by results.

The first package check caught synthetic test fixtures in an interim sdist produced before the
scratch-ignore update. The subsequent build excludes audit scratch files. Interim artifacts
remain local and ignored; none were published or added to Git.

## Compatibility

AST comparison against HEAD confirms **24 tool/decorated async function argument signatures
unchanged**. Source still imports as v0.4.4; **21 default MCP tools** register. Optional Prometheus
registration and Streamable HTTP initialization are covered by existing passing tests. Raw
PromQL remains unchanged by default; disabling it is explicit policy. No output fields removed.
Log partial reads now return the existing incomplete status rather than discarding all progress.

Python 3.10 grammar/import checks pass on the local 3.11 runtime. Runtime 3.10/3.12/3.13 and
Linux/macOS execution await CI; they are not claimed as locally tested. Existing Flink,
Prometheus and Kubernetes integrations use mock coverage; no live version certification here.

## Test Matrix

| Category | Status | Evidence |
| --- | --- | --- |
| Baseline units | PASS | 300 passed, 0 failed, 13 deselected |
| Final units/security/installers | PASS | 341 passed, 0 failed; 13 live integration tests excluded |
| Security/HTTP/installer subset | PASS | 128 passed, 0 failed |
| Syntax / imports | PASS | 13 production Python files compiled and parsed using Python 3.10 grammar |
| MCP contracts / registration | PASS | 24 unchanged argument signatures; 21 default registrations; initialization unit tests |
| Static analysis | PASS | Bandit 1.9.4: 0 findings; 1 justified line suppression |
| Dependency audit | PASS | Installed and locked-export audits: 0 known vulnerabilities |
| Packaging | PASS | Local sdist and wheel built; 2/2 metadata checks pass; scratch filenames excluded in rebuilt artifacts |
| Installer checks | PASS | 2/2: POSIX syntax and mocked Windows invocation; no actual tool installation or client registration |
| Workflow syntax | PASS | YAML parsing of both workflows and Dependabot; remote GitHub execution not performed |
| Live Flink/Prometheus/Kubernetes | SKIP | 13 marked tests excluded; suitable isolated live environment not used |
| Full installer/bootstrap execution | SKIP | Would change user tool installation/client registration; mocked checks used |
| Other Python/OS combinations, minimum SDK 1.23 | SKIP locally | Expanded CI matrix and existing oldest-sdk job retained; not executed here |
| PyPI publisher/environment validation | SKIP | No remote account or infrastructure changes authorized |

Initial development failures (oversized pytest parameter IDs on Windows, a mock PowerShell scope
error and an assertion expecting plugin stderr) were corrected; no failing checks were suppressed.

## Remaining Risks

- Prometheus cardinality/evaluation cost requires server-side sample limits, tenant scope and
  operator-managed quotas. Per-client concurrency does not aggregate limits across distinct
  configured credentials/endpoints or independent server processes. Caller tasks can still
  accumulate until their request deadlines; deployment ingress rate limits remain necessary.
- HTTP is plaintext; TLS, explicit Host allowlists, network access and token log handling are
  deployment responsibilities. An authenticated LAN request with no allowlist accepts any Origin
  by existing design; knowledge of the token grants access. Reverse proxies must preserve auth
  and must not expose an alternate route bypassing the middleware.
- HTTPX decompression/transport buffers exist below application byte caps. Cooperative async
  cancellation cannot interrupt arbitrary blocking codecs or custom transports. General Flink
  and Kubernetes JSON calls retain inactivity timeouts rather than new total deadlines.
- Kubernetes credentials/configuration and exec plugins are trusted. Namespace selection is not
  RBAC. Service-proxy GET permission has a broader upstream surface than fixed MCP tools.
- Discovery closes stale Flink clients, but refresh can race an already in-flight stale client
  request. Registry shutdown is sequential; a client close failure can prevent later closes.
  These lifecycle edge cases were inspected but not reproduced in normal HTTPX shutdown tests.
- Successful upstream data can contain secrets or prompt injection; authorization is shared by
  all users with the MCP bearer token. There is no per-user or per-tool authorization system.
- Mutable third-party Action refs, unpinned isolated build backend, and remote bootstrap trust
  remain. Runtime lock hashes are integrity checks, not proof of publisher trust. Bit-for-bit
  reproducible release builds and remote Trusted Publisher settings were not independently proven.

## Release Recommendation

**CONDITIONALLY READY** for a controlled deployment with least-privilege upstream identities,
TLS and restricted network access. Require the CI/platform/integration evidence before claiming
broader release compatibility. Review remaining release-chain trust and actual PyPI environment
protections before publication. No enterprise-grade or vulnerability-free claim is supported.

## License & Attribution

Added `NOTICE` naming **Hakkı Burak Alkış**, copyright 2026, under the Apache License 2.0,
as requested. README links to both legal files and displays the copyright holder. The existing
`license = "Apache-2.0"` declaration is retained; `license-files` now explicitly lists
`LICENSE` and `NOTICE`. No source-file headers or third-party attributions were removed or changed.

The existing complete `LICENSE` is preserved byte-for-byte against Git HEAD. SHA-256:
`cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30`.
The final wheel and sdist are inspected for both files, checked against the repository bytes,
and their metadata checked for `License-Expression: Apache-2.0` and both `License-File` entries.
This verifies attribution packaging without changing application behavior.

## Git Safety

The initial local audit completed without a commit, push, tag, release, publication or deployment.
The subsequent review handoff explicitly authorizes committing these reviewed changes on
`review/security-hardening-v0.4.5`, pushing only that branch and opening a Draft PR targeting
`main`. The local `main` branch remains at baseline commit
`7364eb1a77efa8131b7fa24669ed585ea26c9ec2`. The source version remains 0.4.4 pending review;
the review branch name does not constitute a version bump or release.

Pre-commit revalidation: **341 passed, 0 failed, 13 deselected** with
`pytest -q -p no:cacheprovider -m "not integration and not k8s"`. The 13 deselected live tests
are reported as SKIP in the review matrix, not as successful integration executions.
No merge, tag, release, PyPI publication, deployment, production cluster mutation, Docker startup,
system service change or external infrastructure creation is authorized by the handoff. Existing
ignored artifacts, caches and temporary files are excluded from the commit. Independent GPT-6
review is required before further changes.
