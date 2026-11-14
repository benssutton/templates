# Findings: triage rubric and false-positive mechanisms

## Contents
1. Where findings come from
2. Triage rubric
3. Marking false positives (local suppression + GitHub dismissal)
4. Fixer brief template

## 1. Where findings come from

| Source | How to get it | Typical finding |
|---|---|---|
| Local service steps | `run_CICD.py <svc> ci` summary + log | lint/format/type errors, test failures, coverage gate, image scan CVEs, k6 thresholds |
| Local security | `run_CICD.py security` | Trivy fs (vuln/secret/misconfig), OSV, Gitleaks, Semgrep |
| GitHub Dependabot alerts | `github_findings.py` → `dependabot_alerts` | vulnerable dependency with a patched version |
| GitHub code scanning | `github_findings.py` → `code_scanning_alerts` | CodeQL, plus Trivy/Semgrep SARIF uploaded by CI |
| GitHub secret scanning | `github_findings.py` → `secret_scanning_alerts` | committed credential |
| GitHub Actions | `github_findings.py` → `workflow_runs`; `gh run view <id> --log-failed` | job failure |
| PR | `github_findings.py --pr N` → `checks`, `unresolved_review_threads` | failing check, reviewer comment |

The same issue often shows up twice, for example in local Trivy and in GitHub code scanning via the uploaded SARIF. De-duplicate by (rule/CVE, file, line) and triage the issue once.

## 2. Triage rubric

Ask, in order:

1. **Is it real?** Reproduce or read the code. A scanner match on a test fixture, a dev-only self-signed cert under `certs/`, or an `.env.example` placeholder isn't a real secret. Gitleaks already allowlists those paths.
2. **Is it reachable or exploitable here?** For a CVE: is the vulnerable function used, and is the dependency runtime or test/dev scope? Is the service exposed in a way that matters? Reachability is the strongest false-positive evidence. Severity alone isn't.
3. **Is there a fix?** A patched version, a config change or a code change. If a fix exists and is cheap, fix it even when exploitability is doubtful. Upgrading is cheaper than defending a suppression later.
4. **Is it in scope?** A major migration, a redesign or something needing the user's decision should be `defer`, with a reason.

Verdict `false-positive` needs a written justification that a reviewer could check in a minute: file/line, the reason, and the evidence. If you can't write one, the verdict is `fix` or `defer`.

Pipeline failures (lint, types, tests, coverage) are almost never false positives. They are the guardrails. Fix the code, not the gate.

## 3. Marking false positives

Suppress at the **source** (so local runs and the next CI upload stop reporting it) **and** dismiss on GitHub (so the alert list is clean). Each local suppression carries a reason and a review/expiry date.

### Local suppression files (repo root)

| Tool | Where | Format |
|---|---|---|
| Trivy | `.trivyignore` | `CVE-2026-1234   # not reachable: <why>; review 2027-02` (also misconfig IDs like `AVD-DS-0002`) |
| OSV-Scanner | `osv-scanner.toml` | `[[IgnoredVulns]]` with `id = "GHSA-…"`, `ignoreUntil = 2027-02-28`, `reason = "…"` |
| Gitleaks | `.gitleaks.toml` `[allowlist]` (paths/regexes), or an inline `gitleaks:allow` comment on the line | add a `description` / comment saying why |
| Semgrep | inline `# nosemgrep: <rule-id>` (or `// nosemgrep: …`) on the flagged line, with a reason comment beside it | prefer the specific rule id over a bare `nosemgrep` |
| SonarQube | mark in the Sonar UI and record in `sonar/false-positives.md` (if present) | rule, file, reason |

Use 3 months as the default review horizon, so suppressions get revisited instead of rotting.

### GitHub dismissal (`gh api`, repo = `owner/name`)

```bash
# Code scanning (CodeQL / uploaded SARIF). reason: "false positive" | "won't fix" | "used in tests"
gh api -X PATCH repos/OWNER/REPO/code-scanning/alerts/N \
  -f state=dismissed -f dismissed_reason="false positive" -f dismissed_comment="<justification>"

# Dependabot alert. reason: fix_started | inaccurate | no_bandwidth | not_used | tolerable_risk (comment ≤ 280 chars)
gh api -X PATCH repos/OWNER/REPO/dependabot/alerts/N \
  -f state=dismissed -f dismissed_reason=not_used -f dismissed_comment="<justification>"

# Secret scanning. resolution: false_positive | wont_fix | revoked | used_in_tests
gh api -X PATCH repos/OWNER/REPO/secret-scanning/alerts/N \
  -f state=resolved -f resolution=false_positive -f resolution_comment="<justification>"
```

Don't dismiss an alert that will be fixed by a pending change. It closes on its own once the fix reaches the default branch, and dismissing it loses that signal. Never dismiss a secret alert for a credential that looks real: that requires rotation, which is the user's job.

## 4. Fixer brief template

```
You are fixing CI/CD findings in <repo path> (Python interpreter: <path from CLAUDE.md>).
Scope: only edit files under <dir(s)>. Other agents are editing other directories in parallel.
Findings:
  - <id> <source> <file:line> <message>  — triage note: <proposed fix>
Guardrails: fix the cause. Don't lower coverage thresholds, skip/delete tests, or add blanket
suppressions (# type: ignore, noqa, @SuppressWarnings, allow(...)). If the right fix needs a
design decision or touches files outside your scope, stop and report that instead.
Read the service's CLAUDE.md before changing it.
Verify with: <targeted run_CICD.py command(s)>
Don't commit. Report: files changed, what you did per finding, verification output summary (PASS/FAIL).
```
