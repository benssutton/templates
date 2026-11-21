package com.example.template.controller;

import static com.example.template.support.SolaceSupport.SEMP;
import static org.awaitility.Awaitility.await;

import com.example.template.dto.CachedDataRowsResponse;
import com.example.template.persistence.streamstore.LsmRow;
import com.example.template.support.ArrowBatches;
import com.example.template.support.IntegrationSupport;
import com.example.template.support.SolaceSupport;
import com.example.template.support.SolaceSupport.Names;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Solace Guaranteed message -> settle-after-write -> /data/cache (append-only Java store). */
// Needs the Solace broker: the IT suffix runs it under failsafe (verify), not surefire (test).
@MicronautTest
class SolaceIngestEndToEndIT extends IntegrationSupport {

    private static final Names NAMES = Names.unique();

    @Inject
    @Client("/")
    HttpClient client;

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> props = super.getProperties();
        props.put("template.ingest.transport", "solace");
        props.put("template.ingest.solace.host", SolaceSupport.SOLACE.getHost());
        props.put(
                "template.ingest.solace.port",
                String.valueOf(SolaceSupport.SOLACE.getMappedPort(55555)));
        props.put("template.ingest.solace.topic", NAMES.topic());
        props.put("template.ingest.solace.queue", NAMES.queue());
        return props;
    }

    // No manual teardown: @MicronautTest stops the context after the class, and the
    // ingest service's preDestroy closes the Solace session (StreamIngestShutdownIT).
    @Test
    void publishedMessagesReachCacheAndAreAckedAfterWrite() throws Exception {
        await().atMost(30, TimeUnit.SECONDS).until(() -> SEMP.queueExists(NAMES.queue()));
        SolaceSupport.publish(
                NAMES.topic(),
                ArrowBatches.encode(
                        List.of(
                                new LsmRow(1, "alpha", "v1", 0, "upsert"),
                                new LsmRow(2, "beta", "v1", 0, "upsert"))));
        SolaceSupport.publish(
                NAMES.topic(),
                ArrowBatches.encode(List.of(new LsmRow(1, "alpha", "v2", 0, "upsert"))));

        await().atMost(30, TimeUnit.SECONDS)
                .until(() ->
                        client.toBlocking()
                                        .retrieve(
                                                HttpRequest.GET("/data/cache?limit=100"),
                                                CachedDataRowsResponse.class)
                                        .total()
                                == 3);
        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> SEMP.queueMessageCount(NAMES.queue()) == 0);
    }
}
