"""Which services does a set of changed files affect? Pure logic plus a git wrapper."""

from __future__ import annotations

import subprocess
from pathlib import Path

from cicd.common import ROOT
from cicd.services import (GLOBAL_PATTERNS, IGNORED_PREFIXES, IGNORED_SUFFIXES,
                           SERVICES)


def _norm(path: str) -> str:
    """Use forward slashes and drop a leading './'."""
    path = path.replace("\\", "/")
    return path.removeprefix("./")


def _is_global(f: str) -> bool:
    """Directory patterns (ending '/') match by prefix; file patterns match exactly."""
    return any(f.startswith(p) if p.endswith("/") else f == p for p in GLOBAL_PATTERNS)


def affected(files: list[str]) -> list[str]:
    """Sorted service names touched by `files`."""
    hit: set[str] = set()
    for raw in files:
        f = _norm(raw)
        if f.endswith(IGNORED_SUFFIXES) or f.startswith(IGNORED_PREFIXES):
            continue
        if _is_global(f):
            return sorted(s.name for s in SERVICES)
        for s in SERVICES:
            if f.startswith(s.path + "/"):
                hit.add(s.name)
    return sorted(hit)


def changed_files(base: str, root: Path = ROOT) -> list[str]:
    """Files changed vs `base` (committed + uncommitted + untracked)."""

    def git(*args: str) -> list[str]:
        out = subprocess.run(["git", *args], cwd=root, capture_output=True, text=True, check=True)
        return [line for line in out.stdout.splitlines() if line]

    files = set(git("diff", "--name-only", base))  # base..working tree
    files |= set(git("ls-files", "--others", "--exclude-standard"))
    return sorted(files)
