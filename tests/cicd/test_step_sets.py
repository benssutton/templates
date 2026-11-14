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
