---
name: cicd-pipeline
description: Run this repo's full CI/CD loop end to end — check scanner/linter versions for upgrades, triage and apply Dependabot updates, run the local pipeline (run_CICD.py), triage and fix findings from local scans and GitHub (code scanning, Dependabot and secret alerts, failed checks), then optionally push to main or raise a PR and keep fixing until the GitHub pipeline is green. Use this skill whenever the user asks to run the pipeline / CI / CICD / checks / scans, to "get the build green", to deal with Dependabot PRs or security alerts, or to push, ship, merge, or open a PR to main in this repo — even if they only mention one of those parts, because pushing without the local loop is how broken builds reach main.
---

# CI/CD pipeline loop

This repo's pipeline is optimised for **comprehensive checks with short iteration cycles**: everything that GitHub runs can be run locally (`run_CICD.py`), so most problems should be found and fixed before anything is pushed. Your job is to orchestrate that loop, delegating each step to a subagent with the model and effort the user chose for it, and to keep a clear record so the final report is accurate.

Read the repo's `CLAUDE.md` and the `Pipeline` section of `README.md` first if you haven't this session. Use the Python interpreter named in `CLAUDE.md` for every `python` command below.

## Scope: what the user asked for

- "Run the pipeline / CI / checks", "fix the findings", "sort out Dependabot" → steps 1–5, then stop and report. Don't commit or push. End by offering to push or raise a PR.
- "Push to main", "ship it", "raise / open a PR to main" → steps 1–7. Asking for the push or PR is the permission for steps 6–7, including commits, pushing, PR creation and dismissing GitHub alerts as false positives.

Step 1 is the only step that asks permission. Everything else runs without stopping, because the user wants short cycles and has pre-approved those actions. Stop and ask only in the situations listed under **When to stop and ask**.

## Orchestration model

You (the main session) are the orchestrator. Subagents can't spawn their own subagents, so you dispatch every agent, including the fixers, and they report back to you. Use the `Agent` tool with these settings. `sonnet` and `opus` always resolve to the latest version of each model.

| Step | Agent | model | effort |
|---|---|---|---|
| 1 Tool versions | `general-purpose` | `sonnet` | `medium` |
| 2 Dependabot | `general-purpose` | `sonnet` | `medium` |
| 3 Local pipeline | `general-purpose` | `sonnet` | `medium` |
| 4 Triage + fixers | `general-purpose` | `opus` | `medium` |
| 6 Push / PR / monitor | `general-purpose` | `sonnet` | `medium` |
| 7 Remote triage + fixers | `general-purpose` | `opus` | `medium` |

The reason for delegating is cost, and for keeping your context clean: pipeline logs and scanner output are long, so the runner agents keep them in log files and return only summaries. Give every agent the repo path, the interpreter path, the run-ledger path, and exactly what to return.

### Run ledger

Create a ledger file at the start, in the session scratchpad (or a temp dir if there is none). Name it `cicd-run-<timestamp>.md` and put pipeline logs next to it. Keep it updated after every step:

- scope (local-only, push to main, or PR), the starting branch, and `git status` at start;
- approved tool upgrades, plus Dependabot updates applied or deferred, each with its reason;
- one row per finding: id, source, verdict (`fix` / `false-positive` / `defer`), evidence, and status;
- local iteration count, remote cycle count, and the final pipeline result per service.

The ledger is how you avoid re-triaging the same finding, how you notice a fix that isn't sticking, and the source for the final summary.

## Preflight (seconds, do it yourself)

Before dispatching anything, check the things that would otherwise fail late or make the result meaningless:

- `docker info` succeeds. The image, k6 and security steps all need Docker, and failing ten minutes into a run wastes the cycle.
- `gh auth status` is logged in. Steps 2, 4, 6 and 7 depend on it.
- `.github/workflows/` exists, on this branch or on `origin/main`. If a push or PR is in scope and there are no workflows, a pushed commit or PR runs no real CI, and a "green" result would mean nothing was checked. Say so in the step-1 question round, so the user can decide to go ahead, knowing the local run is the only gate, or to add workflows first. Don't discover this after pushing.
- `python run_CICD.py list` shows which services are `ready` (blocking) and which are `wip` (non-blocking).

## Step 1: Tool versions (ask permission)

Dispatch the step-1 agent. Have it run `python .claude/skills/cicd-pipeline/scripts/tool_versions.py`, which compares the pinned scanner images in `cicd/steps/tools.py`, the `>=` floors in the `requirements-dev.txt` files, GitHub Actions and pre-commit hooks against their latest upstream releases. It should also check the toolchains the script doesn't cover: the Maven wrapper version, the JDK / Rust / Python base images in the Dockerfiles, and `rust-toolchain.toml` if one exists. Dependabot covers Docker and Maven plugins once `dependabot.yml` exists, so the agent only needs to flag what Dependabot misses.

For each tool that is behind, the agent should read the release notes between the current and latest versions and return a recommendation:

- **recommend**: stable (not an rc, beta or pre-release), and at least 7 days old, which matches the Dependabot cooldown. This lets regressions in brand-new releases surface first. It must also have no breaking changes that affect how `cicd/` invokes the tool (CLI flags, config file format, exit codes, SARIF output).
- **recommend with changes**: worth taking, but `cicd/` or a config file needs adjusting. Say exactly what.
- **hold**: too new, a pre-release, or a breaking change that isn't worth it yet. Give the reason.

Present the list to the user as a compact table (tool, current → latest, age, recommendation, one-line reason) and ask which to apply with `AskUserQuestion`: all recommended, none, or let me choose. Put everything that needs the user into this one question round, so the rest of the run can go unattended:

- any uncommitted changes that were in the working tree at the start. If a push or PR is in scope, ask whether to include them. Don't sweep someone's unrelated work into a commit silently;
- the preflight warning about missing workflows, if it applies.

To keep cycles short, dispatch the step-2 agent at the same time as the step-1 agent, since they touch different files. While the user decides, step 2 can proceed. Apply the approved upgrades once the user answers (a small Sonnet agent, or directly if it's a couple of one-line edits). A new scanner version may surface new findings; that's expected, and step 4 handles them.

## Step 2: Dependabot (no permission needed)

Have the step-2 agent run `python .claude/skills/cicd-pipeline/scripts/github_findings.py` and read the `dependabot_prs` and `dependabot_alerts` sections. For each Dependabot PR and each open alert:

1. Work out what changes: the package, current → target version, and which service. Cross-reference the alerts. A bump that also closes an advisory is a security fix, not routine maintenance, and that raises its priority even when it's a major version. Check whether the advisory affects code the service actually runs, for example a transport it exposes.
2. Assess whether it is **safe and valid**. Read the changelog or release notes for the version range. Patch and minor bumps without breaking notes are normally safe. Major bumps (for example `rmcp 1.7 → 2.1`) need the API changes checked against how the service uses the crate or library. Apply a major bump if the required code changes are small and well understood. Otherwise defer it, with the reason, rather than half-migrating.
3. Apply it **locally on the current branch**, not by merging the Dependabot PR. This puts all updates through one local pipeline run. Use the ecosystem's own tool so lockfiles stay consistent: `cargo update -p <crate> --precise <ver>` (or edit `Cargo.toml` for a major bump), the `pom.xml` version property, or `requirements*.txt`.
4. Security alerts take priority. For an alert with a patched version, upgrade to at least that version, even if no Dependabot PR exists. For a transitive dependency, update the lockfile entry, or the parent if that's the only way.

The agent returns, per item: applied / deferred (with reason) / not applicable, plus the files changed. For every major bump and every deferral, it also returns a short assessment: which APIs changed, which of them the service uses, and what code changed as a result (or why none needed to). The final report needs this. Don't close Dependabot PRs by hand. Dependabot closes them itself once main contains the update, and if the work goes up as a PR, list them in its description ("Supersedes #2, #4").

## Step 3: Local pipeline (no permission needed)

Use the strategy **full → targeted → full**:

- **First iteration: full.** Run three things: `python run_CICD.py all ci`, `python run_CICD.py security`, and `python -m pytest tests/cicd`. The last is the pipeline's own test suite, which no `run_CICD.py` target runs. They are independent, so run them concurrently. If Docker contention makes things flaky, run them one after the other instead.
- **Fix iterations: targeted.** After fixers change something, re-run only what failed and what the fix could affect, for example `python run_CICD.py java test` or `python run_CICD.py security semgrep`. If the change touched `cicd/`, `run_CICD.py` or root tool pins, that affects every service, so go back to a full run. Also re-run `pytest tests/cicd` whenever `cicd/` or `tests/cicd/` changed.
- **Final iteration: full.** Before declaring the local stage green, run the full set once more. Targeted runs can miss a fix that broke something elsewhere.

Tell the runner agent:

- The full `ci` set includes Java Testcontainers, Docker image builds and k6. It can run longer than a foreground shell's timeout, so run commands in the background, redirect output to a log file next to the ledger, and wait for completion. Don't poll.
- Return the per-service summary table that `run_CICD.py` prints (step, PASS/FAIL, seconds) and the security PASS/FAIL lines. For each failure, return the relevant excerpt: the error lines, the failing test names, and each scanner finding's id, file, line and severity. Return excerpts, not the whole log.
- Get service status from `python run_CICD.py list`. A `wip` service (Rust, currently) is non-blocking: still report and triage its failures, but they don't stop the push.
- Attribute each security finding to a path. The scanners are repo-wide, so a `wip` service's lockfile can turn `security` red even though the service itself doesn't block. Report security as two results: findings in `ready` services and the repo root (blocking), and findings only inside `wip` service paths (non-blocking, triaged like any other). Don't add suppressions just to make `security` green. A suppression is only for a genuine false positive, and a real CVE in a `wip` service is still real. If the remote `Pipeline Status` treats security as blocking for the whole repo, say in the final report that the `wip` findings will block remotely, so the user can choose between fixing them now, accepting a red check, or scoping the scan.

## Step 4: Triage and fix (no permission needed)

Collect findings from both sources:

- the local results from step 3;
- GitHub: `github_findings.py` (code-scanning, Dependabot and secret-scanning alerts, plus the latest workflow runs for this branch, and `--pr N` for PR checks and unresolved review threads).

Dispatch one **Opus triage agent** with the combined list and the ledger, so it skips findings already decided. Read `references/findings.md` and pass along the relevant parts: the triage rubric and the false-positive mechanisms. It returns one verdict per finding:

- **fix**: a real problem, with a proposed fix and the files involved;
- **false-positive**: with concrete evidence, for example that the code path isn't reachable, the dependency is test-scope only, or the pattern is a deliberate dev-only fixture. "Looks fine" is not evidence. This verdict suppresses a security signal, so the bar is high;
- **defer**: real, but out of scope for this run (for example a major migration). Give the reason. Deferred items appear in the final summary.

Then dispatch **fixer agents** (Opus, medium) for the `fix` items. Group findings so no two concurrent fixers edit the same files; one fixer per service directory, plus one for the repo root and `cicd/`, works well. Run independent groups in parallel. Each fixer gets:

- its findings, the triage notes and its file scope;
- the guardrails: fix the cause, don't silence the check. Don't lower coverage thresholds, don't delete or skip tests, and don't add blanket `# type: ignore`, `noqa`, `@SuppressWarnings` or `allow(...)`. Tests are this repo's main guardrail for AI-written code (see README), so weakening them to go green defeats the purpose;
- the targeted verification command(s) to run before reporting back;
- the instruction not to commit. Return the files changed, what was done and the verification result.

Apply the false-positive verdicts yourself, using the mechanisms in `references/findings.md`, then record everything in the ledger:

- **Local suppression** (an ignore file entry with a justification and review date): always. It is a reviewable repo change, like any other fix.
- **Dismissing the GitHub alert**: only when a push or PR is in scope. Dismissal changes shared state outside the working tree, and on a local-only run the user has said nothing goes out yet. On a local-only run, list the proposed dismissals (alert link, reason) in the final report so they can be done later.

## Step 5: Loop the local stage

Repeat steps 3 → 4 until all of these hold:

- applicable Dependabot updates are applied, and every deferral has a reason;
- the final full run passes for every `ready` service, for `pytest tests/cicd`, and for blocking security findings;
- every finding has a verdict, and every `fix` item is verified.

If a finding survives two fix attempts, stop retrying it. Mark it `defer`, with what was tried, and carry on. Repeating the same failing approach doesn't shorten the cycle. If a `ready` service is still red after that, it goes into the summary instead of looping indefinitely.

Local-only scope ends here: report (see **Final report**) and offer to push or raise a PR.

## Step 6: Push or PR, then monitor (no permission needed)

Read `references/remote.md` for the commands. In outline, the Sonnet agent:

1. Commits in logical units, using the repo's conventional-commit style (`build(deps): …`, `build(cicd): bump scanner images`, `fix(java): …`) and the session's commit attribution conventions. Include the user's pre-existing uncommitted changes only if they said to in step 1.
2. **Push to main:** fetch, bring the branch up to date with `origin/main`, then fast-forward push (`git push origin HEAD:main`). Never force-push. If there are merge conflicts, or branch protection rejects the push, stop and tell the user; don't work around protection.
   **PR:** push the branch, then `gh pr create --base main` with a summary drawn from the ledger.
3. Monitors the triggered workflow runs to completion (`gh run watch --exit-status` or `gh pr checks --watch`, in the background), and returns the run conclusion per job plus the failed-log excerpts (`gh run view <id> --log-failed`).

If no workflow run appears within about two minutes of the push, the repo may have no `.github/workflows` yet. Record "remote CI not configured", skip the monitoring, and still do step 7's alert check.

## Step 7: Remote findings (no permission needed)

Run `github_findings.py --ref <branch>` (add `--pr N` for a PR) and combine it with the step-6 job failures. If nothing is open and every blocking job is green, you're done.

Dependabot alerts only close once the fix reaches the default branch. On a PR, an alert whose fix is already in the PR is not a remaining finding; record it as "fixed in PR #N".

If findings remain, check the cycle counter in the ledger **before** doing anything else. Count each pass through steps 3 → 7 as one remote cycle.

- **Fewer than three cycles used:** run the step-4 triage-and-fix process with the Opus agents. Go back to step 3 (targeted runs, then a full local run), commit, push, and return to step 7. Increment the counter.
- **Three cycles used and still not green:** stop. Don't start a fourth fix round. Write the **For discussion** summary described under **Final report**, raise it with the user, and wait for their decision. If three rounds of fixes haven't worked, the problem is usually a judgement call rather than another fix.

State the cycle count, for example "cycle 2/3", whenever you report progress, so the user can see how close the run is to the limit.

## When to stop and ask

Step 1's upgrade list always needs an answer. Beyond that, stop only for:

- push conflicts, rejected pushes or branch protection;
- a fix that would need a breaking API change or a design decision;
- a secret that appears genuinely leaked. Rotating it is the user's job, and dismissing the alert would hide it;
- the remote cycle limit.

## Final report

Write the final report for the user from the ledger. Keep it scannable:

```
## CI/CD run: <local-only | pushed to main | PR #N> — <GREEN | NOT GREEN>
Tool upgrades: <applied list> | held: <list with reason>
Dependabot: applied <n> (<list>) | deferred <n> (<item: reason>)
Local pipeline (final full run): <service: PASS/FAIL per service>, pipeline tests: <PASS/FAIL>,
  security: <PASS/FAIL per scanner> (blocking) | <n findings in wip services> (non-blocking)
Findings: fixed <n> | false positives <n> (suppressed where; dismissed on GitHub or "proposed dismissals" for local-only) | deferred <n>
Remote: <workflow conclusion / "not configured: local run was the only gate">, cycles used <k>/3
Open items: <anything needing the user's decision>
```

After the summary block, add a **Notable decisions** section. Give each of these 2–3 plain sentences:

- every **major version bump**: say it is a major bump, what the API assessment found (which breaking changes exist and whether the service is affected), and what was changed as a result;
- every upgrade made **because of a security advisory**: which advisory, whether the vulnerable code is reachable in this service, and how it was fixed;
- every **deferral** or **false-positive** verdict on a high or critical finding: the evidence, and what would be needed to resolve it.

The summary block shows what happened, and this section shows why. A reviewer approving the push needs both. A one-line "rmcp 1.7 → 2.1 applied" hides the risk that a major bump carries. For example:

> **rmcp 1.7 → 2.1 (major).** Treated as a major bump and as a security fix: 2.1 closes a high-severity session-leak DoS in the Streamable HTTP transport, which this service exposes at `/mcp`. The 2.0 breaking changes are renames of model and content types that the service doesn't use, so only `Cargo.toml` changed; `rust test` passes.

If the cycle limit was hit, add a **For discussion** section. For each blocking issue, give what fails (with a log excerpt or alert link), what was tried and why it didn't work, the options you see with their trade-offs, and your recommendation.
