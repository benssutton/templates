import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def cli(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, str(ROOT / "run_CICD.py"), *args],
                          cwd=ROOT, capture_output=True, text=True)


def test_list_shows_all_services():
    out = cli("list")
    assert out.returncode == 0
    for name in ("python", "java", "rust"):
        assert name in out.stdout


def test_changed_json_for_head_is_a_list():
    out = cli("changed", "--base", "HEAD", "--format", "json")
    assert out.returncode == 0
    assert isinstance(json.loads(out.stdout), list)


def test_unknown_service_fails():
    out = cli("cobol", "fast")
    assert out.returncode != 0


def test_unknown_step_fails():
    out = cli("python", "no-such-step")
    assert out.returncode != 0
    assert "no-such-step" in out.stderr


def test_sonar_unknown_service_fails_cleanly():
    out = cli("sonar", "nosuchservice")
    assert out.returncode == 1
    assert out.stderr.startswith("error:")
    assert "Traceback" not in out.stderr
