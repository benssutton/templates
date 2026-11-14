# Templates

This repo is intended to serve as a template for services in Python, Rust, Java and Typscript that serve as solid foundations for development with agentic AI.  The templates purposefully do not contain business logic, but do contain wiring to common technologies, implement fully automated tests with covage > 90% and aim to demonstrate simple and transparent patterns.  The objectives include:

1. Provide proven, re-useable boilerplate that covers common technologies.  There is little value spending time or tokens re-creating boilerplate for new applications.  
2. Demonstrates best in class design for testability.  Faithful and comprehensive test coverage is a critical guardrail when using AI.
3. Demonstrate enterprise-level robustness and stability through self-recovery and exposure of observability data points.
4. Serve as a reference for teams (and AI agents) to converge on.  Recognisable patterns facilitate understanding.
5. Separate concerns between modules.  Decoupling infrastructure from domain logic, and further decoupling sub-domains delineates and improves understanding of scope.
6. Use idomatic code wherever possible; and transparent reasoning where not.
7. Through all of the above assist code reviewers pattern-match against what good looks like.
8. Serve as a light-weight educational/debugging tool that developers can easily step through.


# Architectural Decisions

Testing
1. Testing should use real services/dependenices wherever possible.  Avoid mocks and patches.
2. Testing should exercise real endpoints (HTTP, websockets)
3. Tests should be async and run in parallel where possible to shorten the test cycle.
4. Tests should include performance benchmarks
5. Tests should be runnable locally as well as in the build pipeline.  Results of tests should be used as control gates in the CI/CD process.

Languages
1. Compiler support should be leveraged wherever possible.  Pylance and Pyright should be used to support applications in Python

APIs
1. All services should be accessed through REST endpoints: application management, configuration, observability and functional services should all be accessible via REST.
2. MCPs are optional.  MCP Tools should simply mirror REST endpoints.  

Resilience
1. Critical dependencies should be eagerly loaded and smoke-tested on application start (Fail Fast).  Disconnects should be re-tried with exponential back-off.  After a specified number of re-try failures, the application should shut down.

Observability
1. The application should expose endpoints that support optimal monitoring and observability


# Assumed Use Cases & Technologies

## Languages
- Python: exceptionally, where access to analytic, statistical and ML libraries are the key priority
- Rust: exceptionally, for high-CPU or concurrent workloads where use of parallelism and SIMD vectors are able to make a material difference to processing time.
- Typescript: for UIs
- Java: everything else.  The default language.


# Current Progress

The Python Webservice under python-webservice-template is ready for use.

The Java Webservice under java-webservice-template is ready for use — a Micronaut
(Java 25) mirror of the Python template with the same endpoints and feature set.

Other templates that might show in this repo are still works-in-progress.

# Pipeline

One entry point, identical locally and in GitHub Actions: `python run_CICD.py` (use the p315 conda env).

| Command | What it does |
|---|---|
| `run_CICD.py list` | services and whether they are ready or wip |
| `run_CICD.py changed [--base origin/main]` | services affected by your changes |
| `run_CICD.py changed fast` / `check` / `ci` | run a step set on changed services only |
| `run_CICD.py python\|java\|rust <step...>` | one service (`fmt`, `fmt-check`, `lint`, `typecheck`, `test`, `build-image`, `scan-image`, `k6-smoke`, `k6-load`, ...) |
| `run_CICD.py security [trivy\|osv\|gitleaks\|semgrep]` | repo-wide scanners (Docker) |
| `run_CICD.py sonar <service>` | local SonarQube analysis (see sonar/README.md) |

Step sets: **fast** (format + lint, seconds) < **check** (+ tests/coverage) < **ci** (+ image build/scan, k6 smoke).

**Change detection** has one source of truth: `cicd/services.py` + `cicd/changed.py`. Changes under a service directory run that service; changes to `run_CICD.py`, `cicd/`, `.github/`, scanner configs or `requirements-dev.txt` run all; markdown/docs-only changes run no service jobs (CI still starts, so `Pipeline Status` reports and passes).

**GitHub**: `ci.yml` runs on every push to main, every pull request (no `paths-ignore`) and manual dispatch (input `services`: `changed`|`all`); the `pipeline-tests` job always runs `tests/cicd`. If the push base is unknown (force-push) it diffs against `HEAD~1`, and with no usable base it runs all services. There are no scheduled workflows; Dependabot (monthly, all ecosystems) is the only recurring automation. The `Pipeline Status` job is the single required check for branch protection (Settings > Branches > require status check `Pipeline Status`). Rust is wip: it runs but never blocks; set `ready=True` in `cicd/services.py` and add `rust` to `status.needs` when it is ready.

**Local hooks**: `.pre-commit-config.yaml` runs gitleaks on commit and `changed fast` on push (`pre-commit install --install-hooks && pre-commit install --hook-type pre-push`).

**Suppressions** (`.trivyignore`, `osv-scanner.toml`, `.gitleaks.toml`, `sonar/false-positives.md`) must carry a justification for every entry.

## Former GitLab CI jobs

The per-service `.gitlab-ci.yml` files were retired; their jobs map as follows.

| GitLab job | Now |
|---|---|
| Python `pytest` | `run_CICD.py python test` |
| Java `build-test` | `run_CICD.py java check` / `ci` |
| `k6-smoke` | `k6-smoke` step (part of `ci`) |
| `k6-load` | local only: `run_CICD.py <svc> k6-load` |
| `k6-stress` | no pipeline step; run locally with the service's `docker compose up -d --wait`, then `tests/performance/stress.js` via the k6 image (`-e BASE_URL=... -e TARGET_RPS=50`) |

## Known limitations

**Baselined findings.** The first scan's findings are suppressed with time-boxed entries: `.trivyignore` (`exp:`), `osv-scanner.toml` (`ignoreUntil`) and two `# nosemgrep` comments in the service Dockerfiles. All expire on **2026-12-14**, after which the pipeline fails until the follow-up dependency/base-image bump PR lands: jackson, netty, guava, postgresql, micrometer, httpcore5, black, cryptography, Debian base-image CVEs, and a non-root `USER` in both service Dockerfiles. The Rust template crates are WIP.

**Scanner coverage.**

| Scanner | Limit |
|---|---|
| OSV | Maven is scanned via a CycloneDX SBOM generated in Docker |
| OSV / Trivy | the Python service's runtime `requirements.txt` uses ranges and is not scanned (follow-up: lock file) |
| Trivy | cannot see BOM-managed Maven versions |

**Local SonarQube** shares its Docker Compose project name (`sonar`) and volumes with turbo-parakeet's `sonar/compose.yml`: one local SonarQube serves both repos. See sonar/README.md for token setup.

**Rust** is `ready=False` (wip): it runs but never blocks. It currently fails `fmt-check` (32 rustfmt diffs); fix when promoting.

**Not verifiable until the first GitHub run:** CodeQL for Rust, `upload-sarif` (needs code scanning enabled on the repo), and the Docker steps on ubuntu runners. Manual step: require `Pipeline Status` in branch protection.
