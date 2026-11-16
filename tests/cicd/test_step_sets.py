import pytest

from cicd.steps import java_steps, python_steps, rust_steps

MODULES = [python_steps, java_steps, rust_steps]


@pytest.mark.parametrize("mod", MODULES)
def test_sets_only_reference_defined_steps(mod):
    for set_name, steps in mod.SETS.items():
        for s in steps:
            assert s in mod.STEPS, f"{mod.__name__} set {set_name} references unknown step {s}"


@pytest.mark.parametrize("mod", MODULES)
def test_ci_is_superset_of_check_is_superset_of_fast(mod):
    s = mod.SETS
    assert set(s["fast"]) <= set(s["check"]) <= set(s["ci"])


def test_rust_ci_builds_scans_and_smokes():
    from cicd import services

    assert rust_steps.SETS["ci"] == [
        "fmt-check", "lint", "test", "coverage", "build-image", "scan-image", "k6-smoke",
    ]
    assert {"build-image", "scan-image", "k6-smoke", "k6-load"} <= set(rust_steps.STEPS)
    rust = services.get("rust")
    assert rust.compose_network == "rust-template_default"
    assert rust.ready is False  # promoted in R4


@pytest.mark.parametrize("step", ["test", "coverage"])
def test_rust_test_steps_pass_smf_python(monkeypatch, step):
    # tests/it/smf_interop.rs needs SMF_PYTHON; default is the pipeline's interpreter.
    import sys

    from cicd import services

    calls = []
    monkeypatch.setattr(rust_steps, "run", lambda cmd, cwd, env=None: calls.append(env))
    monkeypatch.delenv("SMF_PYTHON", raising=False)
    rust_steps.STEPS[step](services.get("rust"))
    assert calls[0]["SMF_PYTHON"] == sys.executable
    assert "PATH" in calls[0]  # the rest of the environment is kept

    monkeypatch.setenv("SMF_PYTHON", "/opt/other/python")
    rust_steps.STEPS[step](services.get("rust"))
    assert calls[1]["SMF_PYTHON"] == "/opt/other/python"
