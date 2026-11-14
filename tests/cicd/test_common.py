import pytest

from cicd.common import ROOT, StepFailed, docker_run_command, run


def test_run_missing_command_raises_step_failed():
    with pytest.raises(StepFailed, match="cannot execute"):
        run(["cicd-no-such-command-xyz"])


def test_docker_run_command_mounts_repo_and_trusts_it_for_git():
    cmd = docker_run_command("img:1", ["detect"], mounts={"cache": "/c"}, env=["TOKEN"], network="net")
    assert cmd[:3] == ["docker", "run", "--rm"]
    assert ["-v", f"{ROOT}:/src"] == cmd[3:5]
    # The mounted checkout is owned by another uid: without this git refuses it ("dubious ownership").
    for pair in ("GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=safe.directory", "GIT_CONFIG_VALUE_0=/src"):
        i = cmd.index(pair)
        assert cmd[i - 1] == "-e"
    assert ["-v", "cache:/c"] == cmd[cmd.index("cache:/c") - 1:cmd.index("cache:/c") + 1]
    assert cmd[cmd.index("TOKEN") - 1] == "-e"
    assert cmd[cmd.index("--network") + 1] == "net"
    assert cmd[-2:] == ["img:1", "detect"]
