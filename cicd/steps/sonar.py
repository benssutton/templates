"""Local SonarQube analysis (sonar/README.md). Runs the service's tests first for coverage."""

from __future__ import annotations

import os
import subprocess
import sys

from cicd.common import PY, ROOT, run as sh
from cicd.services import Service
from cicd.steps import tools
from cicd.steps.java_steps import _mvnw

COMPOSE = ["docker", "compose", "-f", str(ROOT / "sonar" / "compose.yml")]


def _network() -> str:
    """Docker network of the running SonarQube: the scanner joins it and talks to sonarqube:9000."""
    cid = subprocess.run([*COMPOSE, "ps", "-q", "sonarqube"], capture_output=True, text=True).stdout.strip()
    if not cid:
        sys.exit("SonarQube is not running: docker compose -f sonar/compose.yml up -d")
    nets = subprocess.run(
        ["docker", "inspect", "-f", "{{range $n, $_ := .NetworkSettings.Networks}}{{$n}} {{end}}", cid],
        capture_output=True, text=True).stdout.split()
    if not nets:
        sys.exit(f"cannot find the Docker network of container {cid}")
    return nets[0]


def scanner_command(service: Service, network: str) -> list[str]:
    """The `docker run` for the scanner. The token is forwarded by name (-e SONAR_TOKEN), never by value."""
    return ["docker", "run", "--rm", "--network", network,
            "-e", "SONAR_HOST_URL=http://sonarqube:9000", "-e", "SONAR_TOKEN",
            "-v", f"{ROOT / service.path}:/usr/src", tools.SONAR_SCANNER]


def run(service: Service) -> None:
    token = os.environ.get("SONARQUBE_TOKEN")
    if not token:
        sys.exit("SONARQUBE_TOKEN is not set (see sonar/README.md)")
    network = _network()  # fail fast, before the slow test run
    svc_dir = ROOT / service.path
    if service.language == "python":
        sh([PY, "-m", "pytest", "tests/", "--cov", "--cov-report=xml"], svc_dir)
    elif service.language == "java":
        sh([_mvnw(service), "-B", "verify"], svc_dir)
    # rust: no coverage import, scan only
    sh(scanner_command(service, network), env=dict(os.environ, SONAR_TOKEN=token))
