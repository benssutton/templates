"""Thin SEMP v2 client: the broker-side view and actions a real Solace
operator would use. Tests assert on broker state (queue depth, subscriptions)
rather than trusting the client's own account of what happened."""

from urllib.parse import parse_qs, quote, urlsplit

import httpx


def _error_status(r: httpx.Response) -> str | None:
    try:
        return r.json()["meta"]["error"]["status"]
    except (ValueError, KeyError, TypeError):
        return None


class Semp:
    def __init__(
        self,
        base_url: str,
        vpn: str = "default",
        user: str = "admin",
        password: str = "admin",
    ):
        self._http = httpx.Client(
            base_url=f"{base_url}/SEMP/v2", auth=(user, password), timeout=10.0
        )
        self._vpn = vpn

    def _vpn_path(self, kind: str, collection: str, name: str = "") -> str:
        path = f"/{kind}/msgVpns/{self._vpn}/{collection}"
        return f"{path}/{quote(name, safe='')}" if name else path

    def queue_exists(self, queue: str) -> bool:
        """SEMP v2 reports a missing object as HTTP 400 with
        meta.error.status == "NOT_FOUND" (not 404). Anything else that isn't a
        200 (auth failure, 5xx) is raised rather than read as "absent"."""
        r = self._http.get(self._vpn_path("config", "queues", queue))
        if r.status_code == 400 and _error_status(r) == "NOT_FOUND":
            return False
        r.raise_for_status()
        return True

    def queue_subscriptions(self, queue: str) -> list[str]:
        r = self._http.get(self._vpn_path("config", "queues", queue) + "/subscriptions")
        r.raise_for_status()
        return [s["subscriptionTopic"] for s in r.json()["data"]]

    def queue_message_count(self, queue: str) -> int:
        """Messages currently spooled on the queue, including delivered-but-unacked.

        Counts the monitor msgs collection, following the paging cursor. The
        queue's scalar `spooledMsgCount` is NOT used: on the live broker it is a
        cumulative "ever spooled" counter that does not drop when messages are
        acked."""
        path = self._vpn_path("monitor", "queues", queue) + "/msgs"
        params: dict[str, str | int] = {"count": 100}
        total = 0
        while True:
            r = self._http.get(path, params=params)
            r.raise_for_status()
            body = r.json()
            total += len(body["data"])
            next_uri = body["meta"].get("paging", {}).get("nextPageUri")
            if not next_uri:
                return total
            # nextPageUri is absolute with the broker's own view of its host;
            # reuse just the cursor against our base URL.
            cursor = parse_qs(urlsplit(next_uri).query)["cursor"][0]
            params = {"count": 100, "cursor": cursor}

    def _tx_flows(self, queue: str) -> list[dict]:
        r = self._http.get(
            self._vpn_path("monitor", "queues", queue) + "/txFlows",
            params={"count": 100},
        )
        r.raise_for_status()
        return r.json()["data"]

    def queue_bind_count(self, queue: str) -> int:
        """Consumer flows currently bound to the queue (0 once unbound)."""
        return len(self._tx_flows(queue))

    def queue_unacked_count(self, queue: str) -> int:
        """Messages delivered to the queue's consumer flows but not yet settled,
        summed across flows (0 once every flow is unbound)."""
        return sum(f["unackedMsgCount"] for f in self._tx_flows(queue))

    def client_count(self, client_name: str) -> int:
        """Clients currently connected with exactly this client name."""
        r = self._http.get(
            self._vpn_path("monitor", "clients"),
            params={"where": f"clientName=={client_name}", "count": 100},
        )
        r.raise_for_status()
        return len(r.json()["data"])

    def set_endpoint_create(self, client_profile: str, allowed: bool) -> None:
        r = self._http.patch(
            self._vpn_path("config", "clientProfiles", client_profile),
            json={"allowGuaranteedEndpointCreateEnabled": allowed},
        )
        r.raise_for_status()

    def create_client_profile(self, name: str, *, allow_endpoint_create: bool) -> None:
        r = self._http.post(
            self._vpn_path("config", "clientProfiles"),
            json={
                "clientProfileName": name,
                "allowGuaranteedMsgReceiveEnabled": True,
                "allowGuaranteedMsgSendEnabled": True,
                "allowGuaranteedEndpointCreateEnabled": allow_endpoint_create,
            },
        )
        r.raise_for_status()

    def create_client_username(
        self, name: str, password: str, *, client_profile: str
    ) -> None:
        r = self._http.post(
            self._vpn_path("config", "clientUsernames"),
            json={
                "clientUsername": name,
                "password": password,
                "clientProfileName": client_profile,
                "aclProfileName": "default",
                "enabled": True,
            },
        )
        r.raise_for_status()

    def close(self) -> None:
        self._http.close()
