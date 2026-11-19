package com.example.template.ingestion;

import io.micronaut.core.annotation.Nullable;

/**
 * One transport message: its encoded payload plus broker settlement. StreamIngestService checks the
 * size, decodes and writes it, then settles it exactly once:
 *
 * <ul>
 *   <li>{@link #accept()}: ACCEPTED, removed from the queue
 *   <li>{@link #fail()}: FAILED, redelivered (after max-redelivery, to the DMQ)
 *   <li>{@link #reject()}: REJECTED, removed (to the DMQ if configured), never redelivered
 * </ul>
 *
 * The payload is decoded by the service, not the transport, so an oversized message is rejected
 * before any decoding memory is spent on it.
 *
 * <p><b>Production queues:</b> configure a max-redelivery count and a DMQ on the queue. The
 * template's auto-created queues (CREATE_ON_START) get the broker defaults: unlimited redelivery
 * and no DMQ, so a message that keeps failing is redelivered forever; the service's settlement
 * backoff only prevents a hot loop.
 */
public interface Delivery {
    /** Arrow IPC stream bytes, as sent. */
    byte[] payload();

    /** The sender's correlation id (Solace: the message's correlation id property), if any. */
    @Nullable
    String correlationId();

    boolean redelivered();

    void accept();

    void fail();

    void reject();
}
