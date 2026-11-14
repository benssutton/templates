import subprocess
from pathlib import Path

import pytest

from cicd.changed import changed_files


def git(root: Path, *args: str) -> None:
    subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *args],
                   cwd=root, check=True, capture_output=True)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    git(tmp_path, "init", "-q", "-b", "main")
    (tmp_path / "a.txt").write_text("a")
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-q", "-m", "init")
    return tmp_path


def test_nothing_changed(repo):
    assert changed_files("HEAD", repo) == []


def test_modified_and_untracked_and_committed(repo):
    (repo / "a.txt").write_text("b")                 # modified, uncommitted
    (repo / "new.txt").write_text("n")               # untracked
    (repo / "c.txt").write_text("c")
    git(repo, "add", "c.txt")
    git(repo, "commit", "-q", "-m", "c")             # committed after base
    assert changed_files("HEAD~1", repo) == ["a.txt", "c.txt", "new.txt"]


@pytest.fixture
def moved(repo: Path) -> Path:
    """svc_a/x.txt committed, then `git mv` to svc_b/x.txt (staged, not committed)."""
    (repo / "svc_a").mkdir()
    (repo / "svc_a" / "x.txt").write_text("x")
    git(repo, "add", ".")
    git(repo, "commit", "-q", "-m", "svc_a")
    (repo / "svc_b").mkdir()
    git(repo, "mv", "svc_a/x.txt", "svc_b/x.txt")
    return repo


def test_uncommitted_rename_lists_old_and_new_paths(moved):
    assert changed_files("HEAD", moved) == ["svc_a/x.txt", "svc_b/x.txt"]


def test_committed_rename_lists_old_and_new_paths(moved):
    git(moved, "commit", "-q", "-m", "move")
    assert changed_files("HEAD~1", moved) == ["svc_a/x.txt", "svc_b/x.txt"]


def test_unknown_base_raises_with_git_message(repo):
    with pytest.raises(RuntimeError, match="no-such-ref") as exc:
        changed_files("no-such-ref", repo)
    assert "fatal" in str(exc.value)   # git's own stderr is surfaced
