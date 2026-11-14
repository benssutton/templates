"""Repo-wide scanners, run as pinned Docker images (identical locally and in CI).

Set CICD_SARIF_DIR to also write SARIF files there (the GitHub workflow uploads them).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from cicd.common import ROOT, StepFailed, docker_run
from cicd.services import SERVICES, Service
from cicd.steps import tools

SKIP_DIRS = "target,node_modules,.venv,__pycache__,.git,.claude"  # top-level names (osv_targets)
# Trivy matches --skip-dirs against full paths, so nested dirs (e.g. <service>/target) need **.
TRIVY_SKIP = "**/target,**/node_modules,**/.venv,**/__pycache__,**/.git,**/.claude"


def _sarif(name: str) -> list[str]:
    d = os.environ.get("CICD_SARIF_DIR")
    return ["--format", "sarif", "--output", f"/src/{d}/{name}.sarif"] if d else []


def trivy_args() -> list[str]:
    # --offline-scan: Trivy never fetches pom.xml parents/BOMs from Maven Central (shared
    # runner IPs get 429-blocked, failing the whole scan). Nothing is lost: Java deps are
    # covered by OSV on Maven's fully resolved SBOM (osv()) and by the Java image scan
    # (images.py); secret/misconfig and other ecosystems' lockfiles need no remote calls.
    # No ~/.m2 mount either, so results don't depend on a warm local cache (local == CI).
    return ["fs", "--scanners", "vuln,secret,misconfig", "--offline-scan",
            "--severity", "HIGH,CRITICAL", "--exit-code", "1",
            "--skip-dirs", TRIVY_SKIP, "--timeout", "15m",
            "--ignorefile", "/src/.trivyignore",
            *_sarif("trivy"), "/src"]


def trivy() -> None:
    docker_run(tools.TRIVY, trivy_args(), mounts={"cicd-trivy-cache": "/root/.cache"})


def maven_sbom_args(service: Service) -> list[str]:
    """`mvn` (run in tools.MAVEN) writing <service>/target/bom.json: the resolved
    compile/runtime dependency tree, so OSV sees the versions Maven actually ships."""
    return ["mvn", "-B", "-q", "-f", f"/src/{service.path}/pom.xml",
            f"{tools.CYCLONEDX_MAVEN_PLUGIN}:makeBom", "-DoutputFormat=json", "-DoutputName=bom"]


def osv_args(sboms: list[str], targets: list[str]) -> list[str]:
    return ["scan", "source", "--recursive", "--config=/src/osv-scanner.toml",
            *(f"-L={s}" for s in sboms), *targets]


def osv_targets(root: Path = ROOT) -> list[str]:
    """Top-level repo entries, minus Maven services: osv-scanner v2.7.0 resolves pom.xml
    itself (parent fetch is flaky, BOM versions differ), so those are scanned by SBOM.
    Gitignored entries are dropped too: osv exits 127 when given one explicitly."""
    skip = {*SKIP_DIRS.split(","), *(s.path for s in SERVICES if s.language == "java")}
    names = [p.name for p in sorted(root.iterdir()) if p.name not in skip]
    done = subprocess.run(["git", "check-ignore", "--no-index", "--stdin", "-z"], cwd=root,
                          input="\0".join(names).encode(), capture_output=True)
    if done.returncode not in (0, 1):  # 1 = nothing ignored
        raise StepFailed(f"git check-ignore failed: {done.stderr.decode().strip()}")
    ignored = set(done.stdout.decode().split("\0"))
    return [f"/src/{n}" for n in names if n not in ignored]


def osv() -> None:
    sboms = []
    for service in (s for s in SERVICES if s.language == "java"):
        docker_run(tools.MAVEN, maven_sbom_args(service), mounts={"cicd-maven-cache": "/root/.m2"})
        sbom = Path(service.path, "target", "bom.json")
        if not (ROOT / sbom).is_file():
            raise StepFailed(f"SBOM not generated: {sbom}")
        sboms.append(f"/src/{sbom.as_posix()}")
    docker_run(tools.OSV, osv_args(sboms, osv_targets()))


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
