"""End to end: Solace Guaranteed message -> settle-after-write -> /data/cache."""

import logging

import pyarrow as pa

from tests.app_client import lifespan_test_client
from tests.publishers.batches import make_batch
from tests.support.solace import wait_for_queue
from tests.support.wait import eventually, poll_cache

# lsm_flush_rows=2, lsm_compaction_runs=2 (app_settings defaults):
#   BATCH_1: 2 rows -> flush -> run1
#   BATCH_2: 2 rows -> flush -> run2 -> compaction: id=1 gets v2
#   BATCH_3: 1 row  -> memtable tombstone beats id=2 in the compacted run
BATCH_1 = make_batch([(1, "alpha", "v1", "upsert"), (2, "beta", "v1", "upsert")])
BATCH_2 = make_batch([(1, "alpha", "v2", "upsert"), (3, "gamma", "v1", "upsert")])
BATCH_3 = make_batch([(2, "beta", "v1", "delete")])


def _values(body: dict) -> dict[int, str]:
    return {r["id"]: r["value"] for r in body["rows"]}


async def test_three_batches_merge_newest_wins_with_tombstone(
    solace_settings, solace_names, semp, solace_publisher
):
    """Publish the three batches ONCE: the durable queue guarantees delivery,
    so no republish-until-seen loop is needed."""
    async with lifespan_test_client(solace_settings()) as client:
        await wait_for_queue(semp, solace_names.queue)
        for batch in (BATCH_1, BATCH_2, BATCH_3):
            solace_publisher.publish_batch(batch)
        # total == 2 also holds after BATCH_1 alone (totals go 2 -> 3 -> 2), so
        # wait for the END state, not just the count.
        body = await poll_cache(
            client,
            expected_total=2,
            until=lambda b: 2 not in _values(b) and _values(b).get(1) == "v2",
        )
    values = _values(body)
    assert values[1] == "v2"  # newest wins across compaction
    assert 2 not in values  # memtable tombstone beats the compacted run
    assert values[3] == "v1"  # unmodified row survives


async def test_message_is_acked_once_written(
    solace_settings, solace_names, semp, solace_publisher
):
    async with lifespan_test_client(solace_settings()) as client:
        await wait_for_queue(semp, solace_names.queue)
        solace_publisher.publish_batch(BATCH_1)
        await poll_cache(client, expected_total=2)
        await eventually(
            lambda: semp.queue_message_count(solace_names.queue) == 0
            and semp.queue_unacked_count(solace_names.queue) == 0,
            what="message acked after write",
        )


async def test_messages_published_while_app_down_are_delivered_on_start(
    solace_settings, solace_names, semp, solace_publisher
):
    settings = solace_settings()
    async with lifespan_test_client(settings):
        await wait_for_queue(
            semp, solace_names.queue
        )  # first start creates the durable queue
    # Make sure the first app's flow is really gone, so nothing can consume
    # the message before the restart.
    await eventually(
        lambda: semp.queue_bind_count(solace_names.queue) == 0,
        what="first app unbound from queue",
    )
    solace_publisher.publish_batch(BATCH_1)  # app is down: the queue holds it
    await eventually(
        lambda: semp.queue_message_count(solace_names.queue) == 1,
        what="message held by durable queue",
    )
    async with lifespan_test_client(settings) as client:
        await poll_cache(client, expected_total=2)


async def test_unmergeable_message_is_rejected(
    solace_settings, solace_names, semp, solace_publisher, caplog
):
    async with lifespan_test_client(solace_settings()) as client:
        await wait_for_queue(semp, solace_names.queue)
        with caplog.at_level(logging.ERROR, logger="services.stream_ingest"):
            solace_publisher.publish_batch(
                pa.record_batch({"name": ["x"], "value": ["v"], "op": ["upsert"]})
            )
            await eventually(
                lambda: semp.queue_message_count(solace_names.queue) == 0,
                what="rejected",
            )
        assert (await client.get("/data/cache?limit=100")).json()["total"] == 0
        assert "rejecting unmergeable message" in caplog.text


async def test_oversized_message_is_rejected(
    solace_settings, solace_names, semp, solace_publisher, caplog
):
    async with lifespan_test_client(
        solace_settings(max_ingest_batch_bytes=1)
    ) as client:
        await wait_for_queue(semp, solace_names.queue)
        with caplog.at_level(logging.ERROR, logger="services.stream_ingest"):
            solace_publisher.publish_batch(BATCH_1)
            await eventually(
                lambda: semp.queue_message_count(solace_names.queue) == 0,
                what="rejected",
            )
        assert (await client.get("/data/cache?limit=100")).json()["total"] == 0
        assert "exceeds max_ingest_batch_bytes" in caplog.text
