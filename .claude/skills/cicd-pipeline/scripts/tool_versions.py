"""Inventory pinned CI/CD tooling and compare each pin with the latest stable upstream release.

Covers what Dependabot does NOT see (scanner images pinned as Python strings, `>=` floors
for linters) plus GitHub Actions and pre-commit hooks when those files exist.
Stdlib-only; uses the authenticated `gh` CLI for GitHub releases and the PyPI JSON API.

Usage (from the repo root):
    python .claude/skills/cicd-pipeline/scripts/tool_versions.py [--format json|text]

"age_days" is the age of the latest release: the pipeline honours a 7-day cooldown
(same as Dependabot) before recommending a brand-new release.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

ROOT = Path(subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True,
                           text=True).stdout.strip() or ".")

# Docker image -> GitHub repo whose releases track the image tag.
IMAGE_REPOS = {
    "aquasec/trivy": "aquasecurity/trivy",
    "ghcr.io/google/osv-scanner": "google/osv-scanner",
    "zricethezav/gitleaks": "gitleaks/gitleaks",
    "semgrep/semgrep": "semgrep/semgrep",
    "sonarsource/sonar-scanner-cli": "SonarSource/sonar-scanner-cli-docker",
}
# Python dev tools that also publish GitHub releases are looked up on PyPI instead (simpler, exact).
REQ_FILES = ["requirements-dev.txt", "*/requirements-dev.txt"]


def gh_latest(repo: str) -> tuple[str | None, str | None]:
    done = subprocess.run(["gh", "api", f"repos/{repo}/releases/latest", "--jq", "[.tag_name,.published_at]"],
                          capture_output=True, text=True)
    if done.returncode:
        return None, None
    tag, published = json.loads(done.stdout)
    return tag, published


def pypi_latest(pkg: str) -> tuple[str | None, str | None]:
    try:
        with urllib.request.urlopen(f"https://pypi.org/pypi/{pkg}/json", timeout=15) as r:
            data = json.load(r)
    except Exception:
        return None, None
    ver = data["info"]["version"]
    files = data.get("releases", {}).get(ver) or [{}]
    return ver, files[0].get("upload_time_iso_8601")


def age(published: str | None) -> int | None:
    if not published:
        return None
    t = dt.datetime.fromisoformat(published.replace("Z", "+00:00"))
    return (dt.datetime.now(dt.timezone.utc) - t).days


def norm(v: str | None) -> str:
    return (v or "").lstrip("v")


def row(component, file, current, latest, published) -> dict:
    cur, lat = norm(current), norm(latest)
    status = "unknown" if not lat else ("current" if cur == lat or lat.startswith(cur + ".") and cur.count(".") == 0
                                        else "behind")
    return {"component": component, "file": file, "current": current, "latest": latest,
            "latest_published": published, "age_days": age(published), "status": status}


def scanner_images() -> list[dict]:
    out = []
    for f in ROOT.glob("cicd/steps/tools.py"):
        for name, image in re.findall(r'^(\w+)\s*=\s*"([^"]+)"', f.read_text(), re.M):
            repo_part, _, tag = image.rpartition(":")
            gh_repo = IMAGE_REPOS.get(repo_part)
            latest, pub = gh_latest(gh_repo) if gh_repo else (None, None)
            out.append(row(f"{name} ({repo_part})", str(f.relative_to(ROOT)), tag, latest, pub))
    return out


def python_tools() -> list[dict]:
    out = []
    for pattern in REQ_FILES:
        for f in ROOT.glob(pattern):
            for line in f.read_text().splitlines():
                m = re.match(r"^([A-Za-z0-9_.\-\[\]]+)\s*(==|>=|~=)\s*([\w.]+)", line.strip())
                if m:
                    pkg = m.group(1).split("[")[0]
                    latest, pub = pypi_latest(pkg)
                    out.append(row(pkg, str(f.relative_to(ROOT)), f"{m.group(2)}{m.group(3)}", latest, pub)
                               | ({"note": "floor pin: installs latest already; raise the floor to lock in"}
                                  if m.group(2) == ">=" else {}))
    for r in out:  # a `>=` floor never blocks the newest release; compare the floor itself
        if r["current"].startswith(">="):
            r["status"] = "current" if norm(r["current"][2:]) == norm(r["latest"]) else "floor-behind"
    return out


def github_actions() -> list[dict]:
    out = []
    for f in sorted((ROOT / ".github" / "workflows").glob("*.y*ml")):
        for repo, ref, comment in re.findall(r"uses:\s*([\w.\-]+/[\w.\-]+)(?:/[\w./\-]+)?@([\w.\-]+)\s*(?:#\s*(\S+))?",
                                             f.read_text()):
            latest, pub = gh_latest(repo)
            out.append(row(f"action {repo}", str(f.relative_to(ROOT)), comment or ref, latest, pub))
    return out


def precommit_hooks() -> list[dict]:
    f = ROOT / ".pre-commit-config.yaml"
    if not f.exists():
        return []
    out = []
    for url, rev in re.findall(r"repo:\s*https://github.com/([\w.\-]+/[\w.\-]+?)(?:\.git)?\s*\n\s*rev:\s*['\"]?([\w.\-]+)",
                               f.read_text()):
        latest, pub = gh_latest(url)
        out.append(row(f"pre-commit {url}", ".pre-commit-config.yaml", rev, latest, pub))
    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--format", choices=["json", "text"], default="text")
    args = p.parse_args()
    rows = scanner_images() + python_tools() + github_actions() + precommit_hooks()
    if args.format == "json":
        json.dump(rows, sys.stdout, indent=2)
        print()
    else:
        for r in rows:
            print(f"{r['status']:13} {r['component']:44} {r['current']:>14} -> {str(r['latest']):<14} "
                  f"age={r['age_days']}d  {r['file']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
