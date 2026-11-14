"""Steps for the Java (Micronaut) service. Quality gates (JaCoCo) live in the pom."""

from __future__ import annotations

from cicd.common import ROOT, WINDOWS, run
from cicd.services import Service
from cicd.steps import images


def _mvnw(s: Service) -> str:
    return str(ROOT / s.path / ("mvnw.bat" if WINDOWS else "mvnw"))


def compile_(s: Service) -> None:
    run([_mvnw(s), "-B", "-q", "-DskipTests", "compile"], ROOT / s.path)


def test(s: Service) -> None:
    # verify = unit + integration tests (Testcontainers; needs Docker) + JaCoCo check.
    run([_mvnw(s), "-B", "verify"], ROOT / s.path)


def build_image(s: Service) -> None:
    images.build(s)


def scan_image(s: Service) -> None:
    images.scan(s)


def k6_smoke(s: Service) -> None:
    images.k6(s, "smoke.js")


def k6_load(s: Service) -> None:
    images.k6(s, "load.js")


STEPS = {
    "compile": compile_, "test": test, "build-image": build_image,
    "scan-image": scan_image, "k6-smoke": k6_smoke, "k6-load": k6_load,
}
SETS = {
    "fast": ["compile"],
    "check": ["compile", "test"],
    "ci": ["compile", "test", "build-image", "scan-image", "k6-smoke"],
}
