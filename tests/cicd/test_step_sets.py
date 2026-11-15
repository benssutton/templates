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
