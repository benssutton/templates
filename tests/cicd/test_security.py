import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

from cicd.steps import tools

pytestmark = pytest.mark.skipif(shutil.which("docker") is None, reason="docker required")


def test_gitleaks_flags_a_planted_secret():
    with tempfile.TemporaryDirectory() as d:
        # Split literal so this test file itself is not flagged.
        key = "AKIA" + "IOSFODNN7EXAMPLE"
        (Path(d) / "x.env").write_text(f'aws_access_key_id = "{key}"\naws_secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"\n')
        out = subprocess.run(
            ["docker", "run", "--rm", "-v", f"{d}:/s", tools.GITLEAKS,
             "detect", "--no-git", "--source", "/s", "--no-banner"],
            capture_output=True, text=True)
        assert out.returncode != 0, out.stdout + out.stderr
