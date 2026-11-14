"""The service registry: the single source of truth for path -> service mapping."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Service:
    name: str            # CLI name and CI matrix key
    path: str            # directory relative to the repo root
    language: str        # python | java | rust
    ready: bool          # False => runs but never blocks Pipeline Status
    compose_network: str # docker compose default network (k6 smoke)
    sonar_key: str


SERVICES: tuple[Service, ...] = (
    Service("python", "python-webservice-template", "python", True,
            "python-template_default", "templates-python"),
    Service("java", "java-webservice-template", "java", True,
            "java-template_default", "templates-java"),
    Service("rust", "rust-webservice-template", "rust", False,
            "", "templates-rust"),
)

# A change to any of these re-runs every service.
GLOBAL_PATTERNS: tuple[str, ...] = (
    "run_CICD.py", "cicd/", ".github/", "requirements-dev.txt",
    ".trivyignore", ".gitleaks.toml", "osv-scanner.toml", ".pre-commit-config.yaml",
)

# Changes that only touch these run nothing.
IGNORED_SUFFIXES: tuple[str, ...] = (".md",)
IGNORED_PREFIXES: tuple[str, ...] = ("docs/", ".claude/", "sonar/", "tests/cicd/")


def get(name: str) -> Service:
    for s in SERVICES:
        if s.name == name:
            return s
    raise KeyError(f"unknown service {name!r}; known: {[s.name for s in SERVICES]}")
