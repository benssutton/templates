"""Steps for the Rust (axum) service. Registered `ready=False`: never blocks Pipeline Status."""

from __future__ import annotations

from cicd.common import ROOT, run
from cicd.services import Service


def _dir(s: Service):
    return ROOT / s.path


def fmt_check(s: Service) -> None:
    run(["cargo", "fmt", "--all", "--", "--check"], _dir(s))


def fmt(s: Service) -> None:
    run(["cargo", "fmt", "--all"], _dir(s))


def lint(s: Service) -> None:
    run(["cargo", "clippy", "--all-targets", "--", "-D", "warnings"], _dir(s))


def test(s: Service) -> None:
    run(["cargo", "test", "--all-targets"], _dir(s))


def coverage(s: Service) -> None:
    # Requires `cargo install cargo-llvm-cov` and the llvm-tools-preview component.
    run(["cargo", "llvm-cov", "--fail-under-lines", "90", "--lcov", "--output-path", "lcov.info"],
        _dir(s))


STEPS = {"fmt": fmt, "fmt-check": fmt_check, "lint": lint, "test": test, "coverage": coverage}
SETS = {
    "fast": ["fmt-check", "lint"],
    "check": ["fmt-check", "lint", "test"],
    "ci": ["fmt-check", "lint", "test", "coverage"],
}
