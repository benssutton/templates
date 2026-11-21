"""The Java and Rust k6 suites are Python's, byte for byte.

The k6 scripts, including the shared HTTP contract (contract.js), run unchanged
against every service apart from BASE_URL (passed with -e by
cicd/steps/images.py) and the compose network (cicd/services.py). If a Python
script changes, copy it to Java and Rust in the same change, or this test fails.
"""

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
PYTHON = ROOT / "python-webservice-template" / "tests" / "performance"
COPIES = {
    "java": ROOT / "java-webservice-template" / "tests" / "performance",
    "rust": ROOT / "rust-webservice-template" / "tests" / "performance",
}

SHARED = [
    "Dockerfile",
    "contract.js",
    "lib/checks.js",
    "lib/serverTiming.js",
    "lib/thresholds.js",
    "smoke.js",
    "load.js",
    "stress.js",
    "solace_cache.js",
    "profile_reads.js",
    "profile_ingest.js",
    "ingest_http.js",
    "data/rows_params.json",
    "data/ingest_batch.ipc",
    "data/contract_batch.ipc",
]


@pytest.mark.parametrize("service", sorted(COPIES))
@pytest.mark.parametrize("name", SHARED)
def test_k6_file_is_pythons(service: str, name: str) -> None:
    copy, python = COPIES[service] / name, PYTHON / name
    assert python.is_file(), f"reference file vanished: {python.relative_to(ROOT)}"
    assert copy.is_file(), f"missing {copy.relative_to(ROOT)} (copy it from Python)"
    # Compare as git stores them: a Windows checkout with core.autocrlf gives a
    # copy CRLF on disk where a .gitattributes pins LF. Binary fixtures (.ipc)
    # are compared exactly.
    copy_bytes, python_bytes = copy.read_bytes(), python.read_bytes()
    if not name.endswith(".ipc"):
        copy_bytes = copy_bytes.replace(b"\r\n", b"\n")
        python_bytes = python_bytes.replace(b"\r\n", b"\n")
    assert copy_bytes == python_bytes, f"{service}: {name} differs from Python's copy"


def test_every_python_k6_script_is_shared_or_deliberately_skipped() -> None:
    # Python-only by design: py-spy (Rust: profile/run_perf.sh), the SMF demo
    # publisher (Rust: the demo-publisher binary), the ClickHouse seed (Rust:
    # scripts/clickhouse-seed.sh) and the Python fixture generator.
    python_only = {
        "profile/run_pyspy.sh",
        "publishers/solace_publisher.py",
        "data/clickhouse-seed.sh",
        "data/generate_ingest_batch.py",
    }
    found = {
        p.relative_to(PYTHON).as_posix()
        for p in PYTHON.rglob("*")
        if p.is_file() and "__pycache__" not in p.parts and "artifacts" not in p.parts
    }
    unaccounted = found - set(SHARED) - python_only
    assert not unaccounted, (
        f"new Python k6 files to copy to Java and Rust or list as Python-only: {unaccounted}"
    )
