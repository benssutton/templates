"""Every relative link in the repo's markdown resolves to a file or directory.

Plans under docs/superpowers and the skill's eval fixtures are excluded: their
links are quoted examples, not navigation.
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
SCHEME = re.compile(r"^[a-z][a-z0-9+.-]*:", re.IGNORECASE)
SKIP_PARTS = {".git", "node_modules", "target", ".venv", "evals", "superpowers"}


def _markdown() -> list[Path]:
    return sorted(
        p for p in ROOT.rglob("*.md") if not SKIP_PARTS & set(p.relative_to(ROOT).parts)
    )


@pytest.mark.parametrize("path", _markdown(), ids=lambda p: p.relative_to(ROOT).as_posix())
def test_relative_links_resolve(path: Path) -> None:
    broken = []
    for target in LINK.findall(path.read_text(encoding="utf-8")):
        if SCHEME.match(target) or target.startswith("#"):
            continue  # a URL or an in-page anchor
        if not (path.parent / target.split("#", 1)[0]).exists():
            broken.append(target)
    assert not broken, f"{path.relative_to(ROOT).as_posix()}: broken links {broken}"
