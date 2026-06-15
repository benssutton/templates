package com.example.template.ingestion;

import com.example.template.persistence.streamstore.LsmRow;

import java.util.List;
import java.util.function.Consumer;

/**
 * Synchronous interface run on the dedicated ingest thread. {@code consume()}
 * blocks, delivering decoded row batches to the sink until {@code close()}
 * unblocks it. Mirrors the Python ingestion/base.py BatchConsumer protocol.
 */
public interface BatchConsumer extends AutoCloseable {

    /** Block, delivering each decoded batch to {@code sink}. Returns on clean end
     *  (e.g. close()); throws on transport failure. */
    void consume(Consumer<List<LsmRow>> sink) throws Exception;

    @Override
    void close();

    ConnectionState connectionState();
}
