package com.example.template.publisher;

import com.example.template.persistence.streamstore.LsmRow;
import com.example.template.support.ArrowBatches;
import com.solace.messaging.MessagingService;
import com.solace.messaging.config.profile.ConfigurationProfile;
import com.solace.messaging.publisher.PersistentMessagePublisher;
import com.solace.messaging.resources.Topic;
import java.util.List;
import java.util.Properties;

/**
 * Demo publisher for docker-compose: continuously publishes Arrow IPC batches to a Solace topic as
 * Guaranteed messages, for the app's ingest queue to consume. Env: SOLACE_HOST, SOLACE_PORT,
 * SOLACE_VPN, SOLACE_USERNAME, SOLACE_PASSWORD, SOLACE_TOPIC, PUBLISH_INTERVAL_MS (default 100).
 */
public final class ExampleSolacePublisher {

    private static final List<List<LsmRow>> BATCHES =
            List.of(
                    List.of(
                            new LsmRow(1, "alpha", "v1", 0, "upsert"),
                            new LsmRow(2, "beta", "v1", 0, "upsert")),
                    List.of(
                            new LsmRow(1, "alpha", "v2", 0, "upsert"),
                            new LsmRow(3, "gamma", "v1", 0, "upsert")),
                    List.of(new LsmRow(2, "beta", "v1", 0, "delete")));

    private ExampleSolacePublisher() {}

    private static String env(String key, String dflt) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? dflt : v;
    }

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty(
                "solace.messaging.transport.host",
                "tcp://" + env("SOLACE_HOST", "localhost") + ":" + env("SOLACE_PORT", "55555"));
        props.setProperty("solace.messaging.service.vpn-name", env("SOLACE_VPN", "default"));
        props.setProperty(
                "solace.messaging.authentication.basic.username", env("SOLACE_USERNAME", "admin"));
        props.setProperty(
                "solace.messaging.authentication.basic.password", env("SOLACE_PASSWORD", "admin"));
        Topic topic = Topic.of(env("SOLACE_TOPIC", "ingest/batches"));
        long intervalMs = Long.parseLong(env("PUBLISH_INTERVAL_MS", "100"));

        MessagingService service =
                MessagingService.builder(ConfigurationProfile.V1)
                        .fromProperties(props)
                        .build()
                        .connect();
        PersistentMessagePublisher publisher =
                service.createPersistentMessagePublisherBuilder().build().start();
        try {
            while (!Thread.currentThread().isInterrupted()) {
                for (List<LsmRow> batch : BATCHES) {
                    publisher.publishAwaitAcknowledgement(
                            service.messageBuilder().build(ArrowBatches.encode(batch)),
                            topic,
                            15_000L);
                    Thread.sleep(intervalMs);
                }
            }
        } finally {
            publisher.terminate(5_000);
            service.disconnect();
        }
    }
}
