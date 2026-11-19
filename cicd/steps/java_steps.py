"""Steps for the Java (Micronaut) service. Quality gates (Spotless, javac lint, JaCoCo) live in the pom."""

from __future__ import annotations

from cicd.common import ROOT, WINDOWS, run
from cicd.services import Service
from cicd.steps import images


def _mvnw(s: Service) -> str:
    return str(ROOT / s.path / ("mvnw.bat" if WINDOWS else "mvnw"))


def _mvn(s: Service, *args: str) -> None:
    run([_mvnw(s), "-B", *args], ROOT / s.path)


def compile_(s: Service) -> None:
    _mvn(s, "-q", "-DskipTests", "compile")


def fmt(s: Service) -> None:
    _mvn(s, "-q", "spotless:apply")


def fmt_check(s: Service) -> None:
    _mvn(s, "-q", "spotless:check")


def lint(s: Service) -> None:
    # javac -Xlint with -Werror over main and test sources (profile `lint` in the pom).
    _mvn(s, "-q", "-Plint", "-DskipTests", "test-compile")


def test_fast(s: Service) -> None:
    # Surefire only: *Test classes; needs Docker, no Solace broker (*IT classes do).
    _mvn(s, "test")


def test(s: Service) -> None:
    # verify = surefire (*Test) + failsafe (*IT, Solace) against Testcontainers + JaCoCo gate.
    _mvn(s, "verify")


def build_image(s: Service) -> None:
    images.build(s)


def scan_image(s: Service) -> None:
    images.scan(s)


def k6_smoke(s: Service) -> None:
    images.k6(s, "smoke.js", "contract.js")


def k6_load(s: Service) -> None:
    images.k6(s, "load.js")


STEPS = {
    "compile": compile_, "fmt": fmt, "fmt-check": fmt_check, "lint": lint,
    "test-fast": test_fast, "test": test, "build-image": build_image,
    "scan-image": scan_image, "k6-smoke": k6_smoke, "k6-load": k6_load,
}
SETS = {
    "fast": ["fmt-check", "lint"],
    "check": ["fmt-check", "lint", "test"],
    "ci": ["fmt-check", "lint", "test", "build-image", "scan-image", "k6-smoke"],
}
