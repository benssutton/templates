"""Repo-wide scanners, run as pinned Docker images (identical locally and in CI).

Set CICD_SARIF_DIR to also write SARIF files there (the GitHub workflow uploads them).
"""

from __future__ import annotations

import os
import sys

from cicd.common import StepFailed, docker_run
from cicd.steps import tools

SKIP_DIRS = "target,node_modules,.venv,__pycache__,.git,.claude"


def _sarif(name: str) -> list[str]:
    d = os.environ.get("CICD_SARIF_DIR")
    return ["--format", "sarif", "--output", f"/src/{d}/{name}.sarif"] if d else []


def trivy() -> None:
    docker_run(tools.TRIVY, ["fs", "--scanners", "vuln,secret,misconfig",
                             "--severity", "HIGH,CRITICAL", "--exit-code", "1",
                             "--skip-dirs", SKIP_DIRS, "--ignorefile", "/src/.trivyignore",
                             *_sarif("trivy"), "/src"],
               mounts={"cicd-trivy-cache": "/root/.cache"})


def osv() -> None:
    docker_run(tools.OSV, ["scan", "source", "--recursive", "--config=/src/osv-scanner.toml", "/src"])


def gitleaks() -> None:
    docker_run(tools.GITLEAKS, ["detect", "--source", "/src", "--config", "/src/.gitleaks.toml",
                                "--redact", "--no-banner"])


def semgrep() -> None:
    args = ["semgrep", "scan", "--config", "p/default", "--error", "--metrics=off",
            "--exclude", "target", "--exclude", "node_modules", "--exclude", ".claude"]
    d = os.environ.get("CICD_SARIF_DIR")
    if d:
        args += ["--sarif", f"--sarif-output=/src/{d}/semgrep.sarif"]
    docker_run(tools.SEMGREP, [*args, "/src"])


STEPS = {"trivy": trivy, "osv": osv, "gitleaks": gitleaks, "semgrep": semgrep}


def run(names: list[str], fail_fast: bool = False) -> bool:
    ok = True
    for name in names or list(STEPS):
        if name not in STEPS:
            print(f"unknown scanner {name!r}; known: {sorted(STEPS)}", file=sys.stderr)
            raise SystemExit(2)
        try:
            STEPS[name]()
            print(f"PASS  {name}")
        except StepFailed as exc:
            print(f"FAIL  {name}: {exc}")
            ok = False
            if fail_fast:
                break
    return ok
