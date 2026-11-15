"""Solace broker helpers: container lifecycle, readiness, partition simulation."""

import time
import urllib.error
import urllib.request
from contextlib import contextmanager

from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError
from solace.messaging.messaging_service import MessagingService
from testcontainers.core.container import DockerContainer

from tests.support.semp import Semp
from tests.support.settings import SolaceNames
from tests.support.wait import eventually

SOLACE_IMAGE = "solace/solace-pubsub-standard:10.26.10"


def solace_broker() -> DockerContainer:
    # Solace PubSub+ runs a Power-On-Self-Test (POST) at startup and shuts down
    # if hard requirements aren't met:
    #   - nofile hard limit must be >= 1048576.
    #   - memory: ~1 GiB even at the minimum scaling tier
    #     (system_scaling_maxconnectioncount=100). A ~2 GiB Docker VM OOM-kills it.
    return (
        DockerContainer(SOLACE_IMAGE)
        .with_exposed_ports(55555, 8080, 5550)
        .with_env("username_admin_globalaccesslevel", "admin")
        .with_env("username_admin_password", "admin")
        .with_env("system_scaling_maxconnectioncount", "100")
        .with_kwargs(
            shm_size="1g",
            ulimits=[
                {"Name": "core", "Soft": -1, "Hard": -1},
                {"Name": "nofile", "Soft": 1048576, "Hard": 1048576},
            ],
        )
    )


def wait_for_solace_ready(container: DockerContainer, timeout: float = 180) -> None:
    # Readiness is not logged to stdout, so poll the guaranteed-messaging
    # health-check port instead: 200 once the broker is active.
    url = (
        f"http://{container.get_container_host_ip()}:"
        f"{int(container.get_exposed_port(5550))}/health-check/guaranteed-active"
    )
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                if resp.status == 200:
                    return
        except (urllib.error.URLError, ConnectionError, OSError) as exc:
            last_err = exc
        time.sleep(2)
    raise TimeoutError(f"Solace health-check not ready within {timeout}s: {last_err}")


def wait_for_solace_messaging(container: DockerContainer, timeout: float = 120) -> None:
    """guaranteed-active only proves the router is up; the message VPN can
    still refuse SMF clients with 'Not ready' (SERVICE_UNAVAILABLE) for a while
    afterwards. Wait until a real client connect succeeds."""
    props = {
        "solace.messaging.transport.host": (
            f"tcp://{container.get_container_host_ip()}:"
            f"{int(container.get_exposed_port(55555))}"
        ),
        "solace.messaging.service.vpn-name": "default",
        "solace.messaging.authentication.scheme.basic.username": "admin",
        "solace.messaging.authentication.scheme.basic.password": "admin",
    }
    deadline = time.monotonic() + timeout
    delay = 0.5
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        svc = MessagingService.builder().from_properties(props).build()
        try:
            svc.connect()
        except PubSubPlusClientError as exc:
            last_err = exc  # VPN not yet serving clients
        else:
            svc.disconnect()
            return
        time.sleep(delay)
        delay = min(delay * 2, 5.0)
    raise TimeoutError(f"Solace messaging not ready within {timeout}s: {last_err}")


def semp_url(container: DockerContainer) -> str:
    return (
        f"http://{container.get_container_host_ip()}:"
        f"{int(container.get_exposed_port(8080))}"
    )


@contextmanager
def broker_paused(container: DockerContainer):
    """Freeze the broker process: a genuine network partition from the
    client's point of view (keepalives go unanswered -> SDK reconnects)."""
    wrapped = container.get_wrapped_container()
    assert wrapped is not None
    wrapped.pause()
    try:
        yield
    finally:
        wrapped.unpause()


async def wait_for_queue(semp: Semp, queue: str, timeout: float = 60.0) -> None:
    """The consumer creates its queue on receiver start (CREATE_ON_START);
    publishing before the queue exists would be discarded by the broker."""
    await eventually(
        lambda: semp.queue_exists(queue), timeout=timeout, what=f"queue {queue} exists"
    )


def create_endpoint_denied_user(semp: Semp, names: SolaceNames) -> tuple[str, str]:
    """A client username whose profile may NOT create queues: binding to a
    missing queue then fails with a permission error."""
    profile, user, password = f"deny-{names.queue}", f"u-{names.queue}", "pw"
    semp.create_client_profile(profile, allow_endpoint_create=False)
    semp.create_client_username(user, password, client_profile=profile)
    return user, password
