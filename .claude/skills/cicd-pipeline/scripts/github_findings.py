"""Collect every open GitHub finding for this repo as one JSON document.

Stdlib-only; shells out to the authenticated `gh` CLI. Each section is fetched
independently: a section that GitHub refuses (feature disabled, no analyses yet,
missing scope) is reported as {"unavailable": "<reason>"} instead of failing the run.

Usage (from the repo root):
    python .claude/skills/cicd-pipeline/scripts/github_findings.py [--ref BRANCH] [--pr N]

--ref   also report the latest workflow runs for this branch (default: current branch)
--pr    also report check status and unresolved review threads for this PR
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from typing import Any


def gh(*args: str) -> Any:
    done = subprocess.run(["gh", *args], capture_output=True, text=True, encoding="utf-8")
    if done.returncode:
        raise RuntimeError((done.stderr or done.stdout).strip().splitlines()[-1] if (done.stderr or done.stdout).strip() else f"gh exit {done.returncode}")
    out = done.stdout.strip()
    return json.loads(out) if out else None


def paged(path: str) -> list[Any]:
    pages = gh("api", "--paginate", "--slurp", path)
    return [item for page in pages or [] for item in (page if isinstance(page, list) else [page])]


def section(fn):
    try:
        return fn()
    except Exception as exc:  # report, don't abort: other sections are still useful
        return {"unavailable": str(exc)}


def dependabot_prs(repo: str) -> list[dict]:
    prs = gh("pr", "list", "--repo", repo, "--state", "open", "--author", "app/dependabot",
             "--limit", "100", "--json", "number,title,headRefName,url,labels,createdAt,files")
    return [{"number": p["number"], "title": p["title"], "branch": p["headRefName"], "url": p["url"],
             "created": p["createdAt"], "files": [f["path"] for f in p.get("files", [])]} for p in prs]


def dependabot_alerts(repo: str) -> list[dict]:
    out = []
    for a in paged(f"repos/{repo}/dependabot/alerts?state=open&per_page=100"):
        adv, vuln = a.get("security_advisory", {}), a.get("security_vulnerability", {})
        out.append({
            "number": a["number"], "url": a["html_url"],
            "package": vuln.get("package", {}).get("name"),
            "ecosystem": vuln.get("package", {}).get("ecosystem"),
            "manifest": a.get("dependency", {}).get("manifest_path"),
            "scope": a.get("dependency", {}).get("scope"),
            "severity": vuln.get("severity"),
            "ghsa": adv.get("ghsa_id"), "cve": adv.get("cve_id"),
            "vulnerable_range": vuln.get("vulnerable_version_range"),
            "patched": (vuln.get("first_patched_version") or {}).get("identifier"),
            "summary": adv.get("summary"),
        })
    return out


def code_scanning(repo: str) -> list[dict]:
    out = []
    for a in paged(f"repos/{repo}/code-scanning/alerts?state=open&per_page=100"):
        rule, inst = a.get("rule", {}), a.get("most_recent_instance", {})
        loc = inst.get("location", {})
        out.append({
            "number": a["number"], "url": a["html_url"], "tool": a.get("tool", {}).get("name"),
            "rule": rule.get("id"), "severity": rule.get("security_severity_level") or rule.get("severity"),
            "description": rule.get("description"), "message": inst.get("message", {}).get("text"),
            "path": loc.get("path"), "line": loc.get("start_line"), "ref": inst.get("ref"),
        })
    return out


def secret_scanning(repo: str) -> list[dict]:
    return [{"number": a["number"], "url": a["html_url"], "type": a.get("secret_type_display_name")}
            for a in paged(f"repos/{repo}/secret-scanning/alerts?state=open&per_page=100")]


def workflow_runs(repo: str, ref: str) -> list[dict]:
    runs = gh("run", "list", "--repo", repo, "--branch", ref, "--limit", "10",
              "--json", "databaseId,name,event,status,conclusion,headSha,url,createdAt")
    latest: dict[str, dict] = {}
    for r in runs:  # newest first; keep the latest run per workflow
        latest.setdefault(r["name"], r)
    return list(latest.values())


def pr_status(repo: str, pr: int) -> dict:
    checks = section(lambda: gh("pr", "checks", str(pr), "--repo", repo, "--json", "name,state,bucket,link"))
    owner, name = repo.split("/")
    query = """query($o:String!,$n:String!,$pr:Int!){repository(owner:$o,name:$n){pullRequest(number:$pr){
      reviewThreads(first:100){nodes{isResolved path line comments(first:1){nodes{author{login} body url}}}}}}}"""
    def threads():
        data = gh("api", "graphql", "-f", f"query={query}", "-F", f"o={owner}", "-F", f"n={name}", "-F", f"pr={pr}")
        nodes = data["data"]["repository"]["pullRequest"]["reviewThreads"]["nodes"]
        return [{"path": t["path"], "line": t["line"], **(t["comments"]["nodes"][0] if t["comments"]["nodes"] else {})}
                for t in nodes if not t["isResolved"]]
    return {"checks": checks, "unresolved_review_threads": section(threads)}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--ref")
    p.add_argument("--pr", type=int)
    args = p.parse_args()

    repo = gh("repo", "view", "--json", "nameWithOwner")["nameWithOwner"]
    ref = args.ref or subprocess.run(["git", "branch", "--show-current"], capture_output=True,
                                     text=True).stdout.strip()
    result: dict[str, Any] = {
        "repo": repo, "ref": ref,
        "dependabot_prs": section(lambda: dependabot_prs(repo)),
        "dependabot_alerts": section(lambda: dependabot_alerts(repo)),
        "code_scanning_alerts": section(lambda: code_scanning(repo)),
        "secret_scanning_alerts": section(lambda: secret_scanning(repo)),
        "workflow_runs": section(lambda: workflow_runs(repo, ref)),
    }
    if args.pr:
        result["pr"] = {"number": args.pr, **pr_status(repo, args.pr)}
    counts = {k: len(v) for k, v in result.items() if isinstance(v, list)}
    result["counts"] = counts
    json.dump(result, sys.stdout, indent=2)
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
