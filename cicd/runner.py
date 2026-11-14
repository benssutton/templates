"""Run steps for one service, aggregating results into a summary."""

from __future__ import annotations

import sys
import time

from cicd.common import StepFailed
from cicd.services import Service
from cicd.steps import java_steps, python_steps, rust_steps

MODULES = {"python": python_steps, "java": java_steps, "rust": rust_steps}


def expand(service: Service, names: list[str]) -> list[str]:
    """Expand set names (fast/check/ci) into step names; reject unknown names."""
    mod = MODULES[service.language]
    steps: list[str] = []
    for n in names or ["ci"]:
        if n in mod.SETS:
            steps += mod.SETS[n]
        elif n in mod.STEPS:
            steps.append(n)
        else:
            known = sorted(set(mod.STEPS) | set(mod.SETS))
            print(f"unknown step {n!r} for {service.name}; known: {known}", file=sys.stderr)
            raise SystemExit(2)
    return list(dict.fromkeys(steps))


def run_steps(service: Service, names: list[str], fail_fast: bool = False) -> bool:
    """Run steps in order; return True if all passed. Prints a summary table."""
    mod = MODULES[service.language]
    results: list[tuple[str, str, float]] = []
    for step in expand(service, names):
        start = time.monotonic()
        try:
            mod.STEPS[step](service)
            status = "PASS"
        except StepFailed as exc:
            print(f"::error::{service.name}/{step}: {exc}", flush=True)
            status = "FAIL"
        results.append((step, status, time.monotonic() - start))
        if status == "FAIL" and fail_fast:
            break
    print(f"\n== {service.name} ==")
    for step, status, secs in results:
        print(f"  {status:4}  {step:<18} {secs:6.1f}s")
    return all(s == "PASS" for _, s, _ in results)
