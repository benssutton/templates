import json
import secrets
import shutil
import string
import subprocess
import tempfile
from pathlib import Path

import pytest

from cicd import services
from cicd.common import ROOT
from cicd.steps import security, tools

needs_docker = pytest.mark.skipif(shutil.which("docker") is None, reason="docker required")


@needs_docker
def test_gitleaks_flags_a_planted_secret():
    with tempfile.TemporaryDirectory() as d:
        # Random GitHub PAT built at runtime: AWS "...EXAMPLE" keys are on gitleaks'
        # built-in allowlist, and a literal token would get this file flagged.
        alphabet = string.ascii_letters + string.digits
        token = "ghp_" + "".join(secrets.choice(alphabet) for _ in range(36))
        (Path(d) / "x.env").write_text(f'GITHUB_TOKEN = "{token}"\n')
        out = subprocess.run(
            ["docker", "run", "--rm", "-v", f"{d}:/s", tools.GITLEAKS,
             "detect", "--no-git", "--source", "/s", "--no-banner"],
            capture_output=True, text=True)
        assert out.returncode != 0, out.stdout + out.stderr


def test_maven_sbom_uses_pinned_plugin_on_the_service_pom():
    args = security.maven_sbom_args(services.get("java"))
    assert args[:2] == ["mvn", "-B"]
    assert "/src/java-webservice-template/pom.xml" in args
    assert f"{tools.CYCLONEDX_MAVEN_PLUGIN}:makeBom" in args
    assert {"-DoutputFormat=json", "-DoutputName=bom"} <= set(args)


def test_osv_targets_leave_maven_poms_to_the_sbom():
    targets = security.osv_targets()
    assert "/src/java-webservice-template" not in targets
    assert "/src/.git" not in targets
    for s in services.SERVICES:
        if s.language != "java":
            assert f"/src/{s.path}" in targets
    assert "/src/requirements-dev.txt" in targets
    assert all((ROOT / t.removeprefix("/src/")).exists() for t in targets)


def test_osv_targets_drop_gitignored_entries():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        subprocess.run(["git", "init", "-q"], cwd=root, check=True)
        (root / ".gitignore").write_text("cache/\n")
        for name in ("app", "cache", "lib"):
            (root / name).mkdir()
        assert security.osv_targets(root) == ["/src/.gitignore", "/src/app", "/src/lib"]


@needs_docker
def test_osv_fails_on_a_vulnerable_sbom_component():
    # Log4Shell (CVE-2021-44228) in a minimal CycloneDX SBOM, scanned with the step's args.
    bom = {"bomFormat": "CycloneDX", "specVersion": "1.5", "version": 1, "components": [
        {"type": "library", "group": "org.apache.logging.log4j", "name": "log4j-core",
         "version": "2.14.1", "purl": "pkg:maven/org.apache.logging.log4j/log4j-core@2.14.1"}]}
    with tempfile.TemporaryDirectory() as d:
        (Path(d) / "bom.json").write_text(json.dumps(bom))
        (Path(d) / "osv-scanner.toml").write_text("")
        args = security.osv_args(["/src/bom.json"], [])
        out = subprocess.run(["docker", "run", "--rm", "-v", f"{d}:/src", tools.OSV, *args],
                             capture_output=True, text=True)
        assert out.returncode == 1, out.stdout + out.stderr
        assert "GHSA-jfh8-c2jp-5v3q" in out.stdout


def test_trivy_fs_never_resolves_poms_against_maven_central(monkeypatch):
    # Maven Central 429s shared CI IPs; Java deps are covered by OSV's SBOM + the image scan.
    calls = []
    monkeypatch.setattr(security, "docker_run", lambda image, args, mounts=None: calls.append((image, args, mounts)))
    security.trivy()
    [(image, args, mounts)] = calls
    assert image == tools.TRIVY and args[0] == "fs" and "--offline-scan" in args
    assert "/root/.m2" not in (mounts or {}).values()  # same result with a cold or warm cache
