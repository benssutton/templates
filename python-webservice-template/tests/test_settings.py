import pytest
from pydantic import ValidationError

from settings import Settings


@pytest.mark.parametrize(
    "overrides",
    [
        {"lsm_flush_rows": 0},
        {"lsm_compaction_runs": 1},
        {"health_check_timeout_seconds": 0},
        {"ingest_retry_base_delay": 10, "ingest_retry_max_delay": 5},
    ],
)
def test_settings_reject_unworkable_values(overrides):
    with pytest.raises(ValidationError):
        Settings(**overrides)


def test_compression_defaults() -> None:
    s = Settings()
    assert s.compression_enabled is True
    assert s.compression_min_bytes == 1024
    assert s.compression_zstd_level == 3
    assert s.compression_require_zstd is False


@pytest.mark.parametrize("level", [0, 20])
def test_zstd_level_outside_1_to_19_fails(level: int) -> None:
    with pytest.raises(ValidationError, match="compression_zstd_level"):
        Settings(compression_zstd_level=level)


def test_negative_min_bytes_fails() -> None:
    with pytest.raises(ValidationError, match="compression_min_bytes"):
        Settings(compression_min_bytes=-1)


def test_strict_mode_needs_compression_enabled() -> None:
    with pytest.raises(ValidationError, match="compression_require_zstd"):
        Settings(compression_require_zstd=True, compression_enabled=False)
