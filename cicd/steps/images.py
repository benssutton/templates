"""Docker image build, Trivy image scan and k6 smoke/load runs for a service."""

from __future__ import annotations

from cicd.common import ROOT, StepFailed, docker_run, run
from cicd.services import Service
from cicd.steps import tools


def tag(s: Service) -> str:
    return f"{s.sonar_key}:ci"


def build(s: Service) -> None:
    run(["docker", "build", "-t", tag(s), "."], ROOT / s.path)


def scan(s: Service) -> None:
    docker_run(tools.TRIVY, ["image", "--severity", "HIGH,CRITICAL", "--exit-code", "1",
                             "--ignorefile", "/src/.trivyignore", tag(s)],
               mounts={"/var/run/docker.sock": "/var/run/docker.sock"})


def k6(s: Service, script: str) -> None:
    if not s.compose_network:
        raise StepFailed(f"{s.name} has no compose network configured")
    cwd = ROOT / s.path
    try:
        run(["docker", "compose", "up", "-d", "--build", "--wait"], cwd)
        run(["docker", "build", "-t", "perf-scripts", "./tests/performance"], cwd)
        run(["docker", "run", "--rm", "--network", s.compose_network,
             "-e", "BASE_URL=https://app", "perf-scripts", "run", f"/scripts/{script}"], cwd)
    finally:
        run(["docker", "compose", "down", "-v"], cwd)
