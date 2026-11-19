"""Bearer-token seam: off by default; when on, every route including /mcp needs a
valid HS256 JWT except /health/live and /health/ready."""

import time

import jwt
import pytest
from pydantic import SecretStr, ValidationError

from settings import Settings
from tests.app_client import lifespan_test_client
from tests.support.settings import app_settings

SECRET = "test-secret-with-at-least-32-bytes!!"
AUDIENCE = "python-template"
ISSUER = "https://issuer.example"


def _token(secret: str = SECRET, **overrides) -> str:
    claims = {"sub": "tester", "aud": AUDIENCE, "iss": ISSUER, "exp": time.time() + 60}
    claims.update(overrides)
    return jwt.encode(claims, secret, algorithm="HS256")


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture(scope="module")
async def auth_client(
    postgres_container, clickhouse_container, test_clickhouse_client, redis_container
):
    settings = app_settings(
        postgres_container,
        clickhouse_container,
        redis_container,
        auth_enabled=True,
        auth_jwt_secret=SecretStr(SECRET),
        auth_jwt_issuer=ISSUER,
        auth_jwt_audience=AUDIENCE,
    )
    async with lifespan_test_client(settings) as client:
        yield client


def _assert_401(response) -> None:
    assert response.status_code == 401
    assert response.json() == {"detail": "Not authenticated"}
    assert response.headers["WWW-Authenticate"] == "Bearer"


async def test_auth_is_off_by_default(test_client):
    assert Settings().auth_enabled is False
    assert (await test_client.get("/config")).status_code == 200


async def test_valid_token_is_accepted(auth_client):
    assert (
        await auth_client.get("/config", headers=_bearer(_token()))
    ).status_code == 200


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Authorization": "Basic dXNlcjpwYXNz"},
        {"Authorization": "Bearer"},
        _bearer(_token(secret="another-secret-of-at-least-32-bytes!")),
        _bearer(_token(exp=time.time() - 60)),
        _bearer(_token(aud="someone-else")),
        _bearer(_token(iss="https://other.example")),
        _bearer("not-a-jwt"),
    ],
    ids=[
        "missing",
        "wrong-scheme",
        "empty-token",
        "bad-signature",
        "expired",
        "wrong-audience",
        "wrong-issuer",
        "malformed",
    ],
)
async def test_rejected_credentials_answer_401(auth_client, headers):
    _assert_401(await auth_client.get("/config", headers=headers))


async def test_mcp_is_covered(auth_client):
    response = await auth_client.post(
        "/mcp/",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        },
        json={"jsonrpc": "2.0", "id": 1, "method": "ping"},
    )
    _assert_401(response)


@pytest.mark.parametrize("path", ["/health/live", "/health/ready"])
async def test_orchestrator_probes_need_no_token(auth_client, path):
    assert (await auth_client.get(path)).status_code != 401


def test_enabling_auth_without_a_secret_fails_at_startup():
    with pytest.raises(ValidationError, match="auth_jwt_secret"):
        Settings(auth_enabled=True)
