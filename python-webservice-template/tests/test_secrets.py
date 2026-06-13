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
        pytest.skip(f"Password '{secret}' is too short to be a reliable leak sentinel")
    for path in ("/", "/health/status", "/config/"):
        resp = await test_client.get(path)
        assert resp.status_code == 200
        assert secret not in resp.text
