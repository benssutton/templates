"""Steps for the Rust (axum) service. Registered `ready=True`: its `ci` set gates Pipeline Status."""

from __future__ import annotations

import os
import sys

from cicd.common import ROOT, run
from cicd.services import Service
from cicd.steps import images


def _dir(s: Service):
    return ROOT / s.path


def _test_env() -> dict[str, str]:
    # tests/it/smf_interop.rs publishes over SMF with Python's Solace SDK;
    # the interpreter running the pipeline has it (p315 locally, CI installs it).
    return {**os.environ, "SMF_PYTHON": os.environ.get("SMF_PYTHON", sys.executable)}


def fmt_check(s: Service) -> None:
    run(["cargo", "fmt", "--all", "--", "--check"], _dir(s))


def fmt(s: Service) -> None:
    run(["cargo", "fmt", "--all"], _dir(s))


def lint(s: Service) -> None:
    # Both feature sets: the default build (no Solace) and everything, so
    # neither rots.
    run(["cargo", "clippy", "--all-targets", "--", "-D", "warnings"], _dir(s))
    run(["cargo", "clippy", "--all-targets", "--all-features", "--", "-D", "warnings"], _dir(s))


def test(s: Service) -> None:
    # One instrumented run with every feature on, which also writes lcov.info;
    # `coverage` gates on its profiles instead of running the suite again.
    # A plain run (not --no-report) clears stale profiles first.
    # Requires `cargo install cargo-llvm-cov` and the llvm-tools-preview component.
    run(["cargo", "llvm-cov", "--all-features", "--all-targets", "--lcov", "--output-path", "lcov.info"],
        _dir(s), env=_test_env())


def coverage(s: Service) -> None:
    # Reports on the profiles the `test` step left behind (run it first).
    run(["cargo", "llvm-cov", "report", "--fail-under-lines", "90", "--summary-only"],
        _dir(s), env=_test_env())


def deny(s: Service) -> None:
    # Advisories, licences, banned crates (aws-lc) and sources; see deny.toml.
    # Requires `cargo install cargo-deny`.
    run(["cargo", "deny", "--all-features", "check"], _dir(s))


def build_image(s: Service) -> None:
    images.build(s)


def scan_image(s: Service) -> None:
    images.scan(s)


def k6_smoke(s: Service) -> None:
    images.k6(s, "smoke.js", "contract.js")


def k6_load(s: Service) -> None:
    images.k6(s, "load.js")


STEPS = {
    "fmt": fmt, "fmt-check": fmt_check, "lint": lint, "test": test, "coverage": coverage,
    "deny": deny,
    "build-image": build_image, "scan-image": scan_image,
    "k6-smoke": k6_smoke, "k6-load": k6_load,
}
SETS = {
    "fast": ["fmt-check", "lint"],
    "check": ["fmt-check", "lint", "test"],
    "ci": ["fmt-check", "lint", "deny", "test", "coverage", "build-image", "scan-image", "k6-smoke"],
}
