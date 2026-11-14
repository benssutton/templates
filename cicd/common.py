"""Shared helpers: repo root, subprocess runner, docker runner."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable
WINDOWS = os.name == "nt"


class StepFailed(Exception):
    """A pipeline step exited non-zero."""


def run(cmd: list[str], cwd: Path = ROOT, env: dict[str, str] | None = None) -> None:
    """Echo and run a command; raise StepFailed if it fails."""
    rel = cwd.relative_to(ROOT) if cwd != ROOT else Path(".")
    print(f"\n$ ({rel}) {' '.join(cmd)}", flush=True)
    try:
        done = subprocess.run(cmd, cwd=cwd, env=env)
    except OSError as e:  # missing binary, or a script without its executable bit
        raise StepFailed(f"cannot execute {cmd[0]}: {e}") from e
    if done.returncode:
        raise StepFailed(f"exit {done.returncode}: {' '.join(cmd)}")


def docker_run_command(image: str, args: list[str], mounts: dict[str, str] | None = None,
                       env: list[str] | None = None, network: str | None = None) -> list[str]:
    """Build the `docker run --rm` command for a pinned tool image with the repo mounted at /src."""
    cmd = ["docker", "run", "--rm", "-v", f"{ROOT}:/src"]
    # The checkout is owned by the host user, not the container's: mark it safe so git inside
    # the container (gitleaks history scan, semgrep) does not refuse it as "dubious ownership".
    for pair in ("GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=safe.directory", "GIT_CONFIG_VALUE_0=/src"):
        cmd += ["-e", pair]
    for host, target in (mounts or {}).items():
        cmd += ["-v", f"{host}:{target}"]
    for name in env or []:
        cmd += ["-e", name]
    if network:
        cmd += ["--network", network]
    return [*cmd, image, *args]


def docker_run(image: str, args: list[str], mounts: dict[str, str] | None = None,
               env: list[str] | None = None, network: str | None = None) -> None:
    """`docker run --rm` a pinned tool image with the repo mounted at /src."""
    run(docker_run_command(image, args, mounts, env, network))
