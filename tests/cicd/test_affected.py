from cicd.changed import affected


def test_single_service_change():
    assert affected(["java-webservice-template/src/main/App.java"]) == ["java"]


def test_two_services_sorted_unique():
    files = [
        "python-webservice-template/main.py",
        "python-webservice-template/core/x.py",
        "java-webservice-template/pom.xml",
    ]
    assert affected(files) == ["java", "python"]


def test_docs_only_changes_nothing():
    files = ["README.md", "docs/x.png", "python-webservice-template/README.md", "CLAUDE.md"]
    assert affected(files) == []


def test_global_file_affects_all_services():
    assert affected(["run_CICD.py"]) == ["java", "python", "rust"]
    assert affected(["cicd/steps/security.py"]) == ["java", "python", "rust"]
    assert affected([".github/workflows/ci.yml"]) == ["java", "python", "rust"]
    assert affected(["requirements-dev.txt"]) == ["java", "python", "rust"]


def test_unrelated_root_file_changes_nothing():
    assert affected(["LICENSE"]) == []


def test_windows_separators_are_normalised():
    assert affected(["rust-webservice-template\\src\\main.rs"]) == ["rust"]


def test_prefix_lookalike_directory_is_not_a_match():
    assert affected(["python-webservice-template-old/x.py"]) == []
