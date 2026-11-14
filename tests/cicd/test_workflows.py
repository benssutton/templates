"""Static guard-rails for .github/workflows (parsed with PyYAML; actionlint covers syntax)."""

import re
from pathlib import Path

import pytest
import yaml

WORKFLOWS = Path(__file__).resolve().parents[2] / ".github" / "workflows"
FILES = sorted(WORKFLOWS.glob("*.yml"))
SHA = re.compile(r"^[^@\s]+@[0-9a-f]{40}$")


def load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def triggers(wf: dict) -> dict:
    """PyYAML (YAML 1.1) parses the key `on` as boolean True."""
    value = wf["on"] if "on" in wf else wf[True]
    return value if isinstance(value, dict) else {name: None for name in ([value] if isinstance(value, str) else value)}


def uses_of(wf: dict) -> list[str]:
    found = []
    for job in wf["jobs"].values():
        if "uses" in job:
            found.append(job["uses"])
        found += [s["uses"] for s in job.get("steps", []) if "uses" in s]
    return found


def test_workflows_exist() -> None:
    names = {f.name for f in FILES}
    assert {"ci.yml", "service-python.yml", "service-java.yml", "service-rust.yml", "security.yml"} <= names


@pytest.mark.parametrize("path", FILES, ids=lambda p: p.name)
def test_no_schedule_trigger(path: Path) -> None:
    assert "schedule" not in triggers(load(path))


@pytest.mark.parametrize("path", FILES, ids=lambda p: p.name)
def test_external_actions_pinned_to_sha(path: Path) -> None:
    for ref in uses_of(load(path)):
        if not ref.startswith("./"):
            assert SHA.match(ref), f"{path.name}: {ref} is not pinned to a 40-hex SHA"


def test_status_job_gates_on_required_jobs() -> None:
    status = load(WORKFLOWS / "ci.yml")["jobs"]["status"]
    assert status["name"] == "Pipeline Status"
    assert "rust" not in status["needs"]
    assert {"changes", "python", "java", "security"} <= set(status["needs"])


def test_ci_triggers_exact() -> None:
    t = triggers(load(WORKFLOWS / "ci.yml"))
    assert set(t) == {"push", "pull_request", "workflow_dispatch"}
    assert t["push"]["branches"] == ["main"]
    assert t["pull_request"]["branches"] == ["main"]


@pytest.mark.parametrize("path", [f for f in FILES if f.name != "ci.yml"], ids=lambda p: p.name)
def test_reusable_workflows_have_workflow_call(path: Path) -> None:
    assert "workflow_call" in triggers(load(path))
