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
