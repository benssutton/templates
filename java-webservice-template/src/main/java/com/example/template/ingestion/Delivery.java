package com.example.template.ingestion;

import com.example.template.persistence.streamstore.LsmRow;

import java.util.List;

/**
 * One transport message: its decoded rows plus broker settlement. Mirrors the
 * Python ingestion/base.py Delivery. StreamIngestService settles each delivery
 * exactly once, after the store write:
 * <ul>
 *   <li>{@link #accept()}: ACCEPTED, removed from the queue</li>
 *   <li>{@link #fail()}: FAILED, redelivered (after max-redelivery, to the DMQ)</li>
 *   <li>{@link #reject()}: REJECTED, removed (to the DMQ if configured), never redelivered</li>
 * </ul>
 *
 * <p><b>Production queues:</b> configure a max-redelivery count and a DMQ on the
 * queue. The template's auto-created queues (CREATE_ON_START) get the broker
 * defaults: unlimited redelivery and no DMQ, so a message that keeps failing is
 * redelivered forever. StreamIngestService waits a jittered, doubling delay
 * (1 s up to 60 s, reset by the next accept/reject) after each {@code fail()};
 * that only prevents a hot redelivery loop, it does not bound redelivery.
 */
public interface Delivery {
    List<LsmRow> rows();
    /** Encoded (wire) size, checked against {@code template.ingest.max-batch-bytes}. */
    int payloadBytes();
    boolean redelivered();
    void accept();
    void fail();
    void reject();
}
