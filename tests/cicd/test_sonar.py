from cicd import services
from cicd.common import ROOT
from cicd.steps import sonar, tools


def test_scanner_command_mounts_the_service_and_pins_the_image():
    s = services.get("java")
    cmd = sonar.scanner_command(s, "net1")
    assert cmd[cmd.index("--network") + 1] == "net1"
    assert f"{ROOT / s.path}:/usr/src" in cmd
    assert cmd[-1] == tools.SONAR_SCANNER


def test_token_is_forwarded_by_name_not_value():
    cmd = sonar.scanner_command(services.get("python"), "n")
    assert "SONAR_TOKEN" in cmd
    assert not any(a.startswith("SONAR_TOKEN=") for a in cmd)


def test_every_service_has_a_properties_file_with_its_key():
    for s in services.SERVICES:
        props = (ROOT / s.path / "sonar-project.properties").read_text()
        assert f"sonar.projectKey={s.sonar_key}" in props
