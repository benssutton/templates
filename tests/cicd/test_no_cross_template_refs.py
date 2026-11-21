"""Service source never refers to another template: each one reads on its own.

Shared k6 scripts (tests/performance) are excluded: they are byte-identical in
all three services and describe all three by design.
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

OTHERS = {
    "python": r"java|rust|micronaut|axum|tokio|cargo|maven|sibling",
    "java": r"python|rust|fastapi|pydantic|axum|tokio|cargo|sibling|container\.py|_track_last_request",
    "rust": r"python|java|fastapi|pydantic|micronaut|maven|sibling",
}
SUFFIXES = {".py", ".java", ".rs", ".yml", ".yaml", ".toml", ".xml", ".properties", ".sh"}
NAMES = {"Dockerfile"}
SKIP_PARTS = {"target", "__pycache__", "performance", "node_modules", ".pytest_cache"}

# path (relative to the service) -> reason; for a reference that must stay
ALLOWED: dict[str, dict[str, str]] = {"python": {}, "java": {}, "rust": {}}

_PENDING = pytest.mark.xfail(strict=True, reason="Phase 6 cleanup in progress")
SERVICES = [
    pytest.param("java", marks=_PENDING),
    pytest.param("python"),
    pytest.param("rust", marks=_PENDING),
]


def _sources(service: str):
    base = ROOT / f"{service}-webservice-template"
    for path in base.rglob("*"):
        if (
            path.is_file()
            and (path.suffix in SUFFIXES or path.name in NAMES)
            and not SKIP_PARTS & set(path.relative_to(base).parts)
        ):
            yield base, path


@pytest.mark.parametrize("service", SERVICES)
def test_no_reference_to_another_template(service: str) -> None:
    pattern = re.compile(rf"\b({OTHERS[service]})\b", re.IGNORECASE)
    hits = []
    for base, path in _sources(service):
        rel = path.relative_to(base).as_posix()
        if rel in ALLOWED[service]:
            continue
        for n, line in enumerate(path.read_text(encoding="utf-8", errors="ignore").splitlines(), 1):
            if pattern.search(line):
                hits.append(f"{rel}:{n}: {line.strip()}")
    assert not hits, f"{service} refers to another template:\n" + "\n".join(hits)
