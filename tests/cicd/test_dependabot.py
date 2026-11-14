"""Dependabot config: monthly everywhere, valid directories, every ecosystem in the repo covered."""
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / ".github" / "dependabot.yml"


def _updates() -> list[dict]:
    return yaml.safe_load(CONFIG.read_text(encoding="utf-8"))["updates"]


def _dirs(ecosystem: str) -> set[str]:
    return {u["directory"] for u in _updates() if u["package-ecosystem"] == ecosystem}


def test_every_entry_is_monthly():
    intervals = {u["schedule"]["interval"] for u in _updates()}
    assert intervals == {"monthly"}


def test_every_directory_exists():
    for u in _updates():
        assert (ROOT / u["directory"].lstrip("/")).is_dir(), u["directory"]


def test_pip_maven_cargo_cover_manifests():
    assert (ROOT / "python-webservice-template/requirements.txt").is_file()
    assert "/python-webservice-template" in _dirs("pip")
    assert (ROOT / "java-webservice-template/pom.xml").is_file()
    assert "/java-webservice-template" in _dirs("maven")
    assert (ROOT / "rust-webservice-template/Cargo.toml").is_file()
    assert "/rust-webservice-template" in _dirs("cargo")


def test_docker_covers_service_dockerfiles():
    for svc in ("python-webservice-template", "java-webservice-template"):
        assert (ROOT / svc / "Dockerfile").is_file()
        assert f"/{svc}" in _dirs("docker")


def test_github_actions_covered():
    assert list((ROOT / ".github" / "workflows").glob("*.yml"))
    assert "/" in _dirs("github-actions")
