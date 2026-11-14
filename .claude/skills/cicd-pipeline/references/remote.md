# Remote: commit, push or PR, monitor

## Commit

Stage paths explicitly (not `git add -A`), so stray files and the user's unrelated edits stay out. Group the commits:

- `build(deps): bump <pkg> <from> → <to> (<service>)`: Dependabot / security updates (one commit per service is fine)
- `build(cicd): bump <tools>`: approved step-1 tool upgrades
- `fix(<service|cicd>): <what>`: finding fixes
- `chore(security): suppress <id> (false positive: <short reason>)`: suppression entries

End each message with the session's commit attribution lines, if any were given.

## Push to main

```bash
git fetch origin
git status -sb                       # confirm branch and ahead/behind
git rebase origin/main               # only local, unpushed commits are rewritten; on conflict: git rebase --abort, stop, ask
git push origin HEAD:main            # fast-forward only. Never --force / --force-with-lease to main
```

If the current branch is already `main`, `git pull --rebase` and then `git push`. If the push is rejected by branch protection or required reviews, don't bypass it. Tell the user and offer a PR instead.

After a rebase onto new upstream commits, run the targeted local checks for whatever came in before pushing, because the local green result no longer covers it.

## Pull request

```bash
git push -u origin HEAD
gh pr create --base main --title "<conventional title>" --body-file <ledger-derived body>
```

PR body: a summary of tool upgrades, Dependabot updates (with "Supersedes #N" for each Dependabot PR covered), fixed findings, suppressions with justifications, deferred items, and the local pipeline result. End it with the session's PR attribution line, if one was given.

If a PR for this branch already exists (`gh pr view --json number,url`), push to it instead of creating a second one.

## Monitor

```bash
gh run list --branch <branch> --limit 5 --json databaseId,name,status,conclusion,headSha,url
gh run watch <id> --exit-status       # run in the background; you'll be notified when it exits
gh pr checks <N> --watch              # PR alternative: waits for all checks
gh run view <id> --log-failed         # excerpts of failing steps only
```

Match runs to the pushed commit (`headSha` = `git rev-parse HEAD`), so a run for an older commit isn't mistaken for this one. The aggregate check is `Pipeline Status`. Jobs for `wip` services may fail without blocking it. Report them, but they don't make the pipeline red.

No run within about two minutes of the push usually means there are no workflows yet (`.github/workflows` absent) or the change was docs-only (`paths-ignore`). Check which, and record "remote CI not configured" or "no CI triggered (docs-only)" in the ledger.
