"""Service source and docs never refer to another template: each reads on its own.

Shared k6 scripts (tests/performance) are excluded: they are byte-identical in
all three services and describe all three by design.
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

OTHERS = {
    "python": r"java|rust|micronaut|axum|tokio|cargo|maven|jvm|spring|sibling",
    "java": r"python|rust|fastapi|pydantic|axum|tokio|cargo|sibling|container\.py|_track_last_request",
    "rust": r"python|java|fastapi|pydantic|micronaut|maven|jvm|spring|sibling",
}
SUFFIXES = {".py", ".java", ".rs", ".yml", ".yaml", ".toml", ".xml", ".properties", ".sh", ".md"}
NAMES = {"Dockerfile"}
SKIP_PARTS = {"target", "__pycache__", "performance", "node_modules", ".pytest_cache"}
# The repo's own tooling, not a comparison: the pipeline command and the interpreter
# the Rust SMF interop test runs.
TOOLING = re.compile(r"python run_CICD\.py|SMF_PYTHON=\S+")

# path (relative to the service) -> reason; for a reference that must stay
ALLOWED: dict[str, dict[str, str]] = {
    "python": {},
    "java": {},
    "rust": {
        "tests/it/smf_interop.rs": "runs a real Python SMF script (SMF_PYTHON); not a cross-template comparison",
    },
}


def _sources(service: str):
    base = ROOT / f"{service}-webservice-template"
    for path in base.rglob("*"):
        if (
            path.is_file()
            and (path.suffix in SUFFIXES or path.name in NAMES)
            and not SKIP_PARTS & set(path.relative_to(base).parts)
        ):
            yield base, path


@pytest.mark.parametrize("service", sorted(OTHERS))
def test_no_reference_to_another_template(service: str) -> None:
    pattern = re.compile(rf"\b({OTHERS[service]})\b", re.IGNORECASE)
    hits = []
    for base, path in _sources(service):
        rel = path.relative_to(base).as_posix()
        if rel in ALLOWED[service]:
            continue
        for n, line in enumerate(path.read_text(encoding="utf-8", errors="ignore").splitlines(), 1):
            if pattern.search(TOOLING.sub("", line)):
                hits.append(f"{rel}:{n}: {line.strip()}")
    assert not hits, f"{service} refers to another template:\n" + "\n".join(hits)
