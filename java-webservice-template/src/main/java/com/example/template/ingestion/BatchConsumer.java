package com.example.template.ingestion;

import java.util.function.Consumer;

/**
 * Synchronous interface run on the dedicated ingest thread. {@code consume()} blocks, delivering
 * deliveries to the sink until {@code close()} unblocks it.
 */
public interface BatchConsumer extends AutoCloseable {

    /**
     * Block, delivering each message to {@code sink}. Returns on clean end (e.g. close()); throws
     * on transport failure.
     */
    void consume(Consumer<Delivery> sink) throws Exception;

    /**
     * As {@link #consume(Consumer)}, and also runs {@code onBound} on the calling thread once the
     * transport is connected and receiving (for Solace: the persistent receiver started). The
     * ingest loop treats that as proof the transport is healthy and resets its failure count and
     * backoff, so a later throw from the same call is a fresh failure, not one more in a row. The
     * default never signals a bind; consumers that can bind override this.
     */
    default void consume(Consumer<Delivery> sink, Runnable onBound) throws Exception {
        consume(sink);
    }

    @Override
    void close();

    ConnectionState connectionState();
}
