import threading

from ingestion.base import ConnectionState
from ingestion.noop.client import NoopBatchConsumer
from settings import Settings


async def test_noop_consumer_idles_connected_until_closed():
    async with NoopBatchConsumer(Settings(ingest_transport="noop")) as consumer:
        assert consumer.connection_state() == ConnectionState.CONNECTED
        produced: list = []
        t = threading.Thread(target=lambda: produced.extend(consumer.deliveries()))
        t.start()
        t.join(0.2)
        assert t.is_alive()  # blocks: no transport, no deliveries
        consumer.close()
        t.join(2)
        assert not t.is_alive()
        assert produced == []
    assert consumer.connection_state() == ConnectionState.DOWN
