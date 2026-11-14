import secrets
import shutil
import string
import subprocess
import tempfile
from pathlib import Path

import pytest

from cicd.steps import tools

pytestmark = pytest.mark.skipif(shutil.which("docker") is None, reason="docker required")


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
