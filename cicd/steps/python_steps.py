"""Steps for the Python (FastAPI) service."""

from __future__ import annotations

from cicd.common import PY, ROOT, run
from cicd.services import Service
from cicd.steps import images


def _dir(s: Service):
    return ROOT / s.path


def fmt_check(s: Service) -> None:
    run([PY, "-m", "black", "--check", "."], _dir(s))


def fmt(s: Service) -> None:  # rewrites files; not in any set
    run([PY, "-m", "black", "."], _dir(s))
    run([PY, "-m", "ruff", "check", "--fix", "."], _dir(s))


def lint(s: Service) -> None:
    run([PY, "-m", "ruff", "check", "."], _dir(s))


def typecheck(s: Service) -> None:
    run([PY, "-m", "pyright", "--pythonpath", PY], _dir(s))  # check against the env running the pipeline


def test(s: Service) -> None:
    # pytest.ini sets asyncio mode; .coveragerc enforces fail_under.
    run([PY, "-m", "pytest", "tests/", "-v", "--cov"], _dir(s))


def build_image(s: Service) -> None:
    images.build(s)


def scan_image(s: Service) -> None:
    images.scan(s)


def k6_smoke(s: Service) -> None:
    images.k6(s, "smoke.js")


def k6_load(s: Service) -> None:  # local / manual only: not in any set
    images.k6(s, "load.js")


STEPS = {
    "fmt": fmt, "fmt-check": fmt_check, "lint": lint, "typecheck": typecheck,
    "test": test, "build-image": build_image, "scan-image": scan_image,
    "k6-smoke": k6_smoke, "k6-load": k6_load,
}
SETS = {
    "fast": ["fmt-check", "lint", "typecheck"],
    "check": ["fmt-check", "lint", "typecheck", "test"],
    "ci": ["fmt-check", "lint", "typecheck", "test", "build-image", "scan-image", "k6-smoke"],
}
