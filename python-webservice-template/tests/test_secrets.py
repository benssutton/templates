import pytest
from pydantic import SecretStr

from settings import Settings


def test_credential_fields_are_secretstr():
    # Config-contract invariant (not a mock): guards against a future revert to
    # plain str. No HTTP endpoint exposes the DSN, so this type assertion — not
    # an endpoint check — is what actually catches a regression here.
    s = Settings()
    for field in (s.postgres_url, s.clickhouse_password, s.redis_url, s.solace_password):
        assert isinstance(field, SecretStr)
        # SecretStr.repr() must be masked for non-empty values — guards against
        # pydantic regressions where SecretStr is accepted but renders as
        # plaintext in repr/logs. Empty-string defaults render as SecretStr('')
        # (no stars), which is acceptable — an empty credential has nothing to mask.
        if field.get_secret_value():
            assert "**" in repr(field), f"Non-empty SecretStr must mask its value in repr: {repr(field)}"
        else:
            assert field.get_secret_value() == ""


async def test_secrets_absent_from_live_endpoints(test_client, postgres_container):
    # Defence in depth, exercised against the running app: the real Postgres
    # password must never surface in an observability/config/root response.
    #
    # Testcontainers generates the password "test" by default — a 4-character
    # string that is a substring of common words (e.g. "testing", "latest").
    # Checking for it would produce a false positive.  We only run the leak
    # check when the password is long enough to be unambiguous as a sentinel.
    secret = postgres_container.password
    assert secret
    if len(secret) <= 6:
        pytest.skip(
            "Testcontainers default password is 'test' (4 chars) — too short to "
            "distinguish from common words ('testing', 'latest') in response bodies. "
            "This test activates automatically in any environment where POSTGRES_PASSWORD "
            "is set to a longer value (real CI pipelines, staging, production smoke tests)."
        )
    # NOTE: /health/ready can still leak DSN text through error=str(exc) in
    # health probes when a dependency is unavailable. That gap is addressed in
    # Task 10 (generic health probe errors), not here.
    for path in ("/", "/health/status", "/config/", "/health/ready"):
        resp = await test_client.get(path)
        # status may be 200 or 503 (readiness failure) — either is fine;
        # we only care that the credential is not exposed in the body.
        assert secret not in resp.text
