package com.example.template.ingestion.solace;

import com.example.template.config.IngestSettings;
import com.example.template.persistence.streamstore.LsmRow;
import com.solace.messaging.MessagingService;
import com.solace.messaging.config.profile.ConfigurationProfile;
import com.solace.messaging.publisher.DirectMessagePublisher;
import com.solace.messaging.resources.Topic;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.solace.Service;
import org.testcontainers.solace.SolaceContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.nio.channels.Channels;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Contract test for {@link SolaceBatchConsumer}. Stands up a real Solace PubSub+
 * broker via Testcontainers, publishes one Arrow IPC message (one row, id=5) to
 * the subscribed topic with the Solace {@link DirectMessagePublisher}, and
 * asserts the consumer decodes at least one row with id=5 from the live message.
 */
@Testcontainers
class SolaceBatchConsumerTest {

    private static final String TOPIC = "ingest/batches";
    private static final String VPN = "default";
    private static final String USER = "admin";
    private static final String PASS = "admin";

    @Container
    static final SolaceContainer SOLACE = new SolaceContainer(
        DockerImageName.parse("solace/solace-pubsub-standard:10.10"))
        .withVpn(VPN)
        .withCredentials(USER, PASS)
        .withTopic(TOPIC, Service.SMF);

    @Test
    void consumesSingleArrowMessageFromBroker() throws Exception {
        URI smf = URI.create(SOLACE.getOrigin(Service.SMF));

        IngestSettings settings = new IngestSettings();
        settings.getSolace().setHost(smf.getHost());
        settings.getSolace().setPort(smf.getPort());
        settings.getSolace().setVpn(VPN);
        settings.getSolace().setUsername(USER);
        settings.getSolace().setPassword(PASS);
        settings.getSolace().setTopic(TOPIC);

        List<LsmRow> collected = new CopyOnWriteArrayList<>();
        SolaceBatchConsumer consumer = new SolaceBatchConsumer(settings);
        try {
            Thread consumeThread = new Thread(() -> {
                try {
                    consumer.consume(collected::addAll);
                } catch (Exception e) {
                    // consume() returns on close(); ignore interruption/teardown noise.
                }
            }, "solace-consume");
            consumeThread.setDaemon(true);
            consumeThread.start();

            // Wait for the service connection, then republish until the consumer
            // receives a message. Direct messaging is fire-and-forget: a message
            // published before the receiver's subscription is fully established is
            // dropped, so a single shot is racy under load. Resending until received
            // is the robust contract.
            await().atMost(20, SECONDS).until(() ->
                consumer.connectionState() == com.example.template.ingestion.ConnectionState.CONNECTED);

            publishUntilReceived(smf, collected);
        } finally {
            consumer.close();
        }

        assertThat(collected).isNotEmpty();
        assertThat(collected).anyMatch(r -> r.id() == 5L);
    }

    private static void publishUntilReceived(URI smf, List<LsmRow> collected) throws Exception {
        Properties props = new Properties();
        props.setProperty("solace.messaging.transport.host", "tcp://" + smf.getHost() + ":" + smf.getPort());
        props.setProperty("solace.messaging.service.vpn-name", VPN);
        props.setProperty("solace.messaging.authentication.basic.username", USER);
        props.setProperty("solace.messaging.authentication.basic.password", PASS);

        MessagingService service = MessagingService.builder(ConfigurationProfile.V1)
            .fromProperties(props)
            .build()
            .connect();
        try {
            DirectMessagePublisher publisher = service.createDirectMessagePublisherBuilder()
                .build()
                .start();
            try {
                byte[] payload = arrowBytes();
                long deadline = System.currentTimeMillis() + 60_000;
                while (collected.isEmpty() && System.currentTimeMillis() < deadline) {
                    publisher.publish(service.messageBuilder().build(payload), Topic.of(TOPIC));
                    Thread.sleep(1_500);
                }
            } finally {
                publisher.terminate(5_000);
            }
        } finally {
            service.disconnect();
        }
    }

    private static byte[] arrowBytes() throws Exception {
        try (org.apache.arrow.memory.RootAllocator alloc = new org.apache.arrow.memory.RootAllocator()) {
            org.apache.arrow.vector.types.pojo.Schema schema = new org.apache.arrow.vector.types.pojo.Schema(java.util.List.of(
                new org.apache.arrow.vector.types.pojo.Field("id", org.apache.arrow.vector.types.pojo.FieldType.notNullable(new org.apache.arrow.vector.types.pojo.ArrowType.Int(64, true)), null),
                new org.apache.arrow.vector.types.pojo.Field("name", org.apache.arrow.vector.types.pojo.FieldType.notNullable(new org.apache.arrow.vector.types.pojo.ArrowType.Utf8()), null),
                new org.apache.arrow.vector.types.pojo.Field("value", org.apache.arrow.vector.types.pojo.FieldType.notNullable(new org.apache.arrow.vector.types.pojo.ArrowType.Utf8()), null),
                new org.apache.arrow.vector.types.pojo.Field("op", org.apache.arrow.vector.types.pojo.FieldType.notNullable(new org.apache.arrow.vector.types.pojo.ArrowType.Utf8()), null)));
            try (org.apache.arrow.vector.VectorSchemaRoot root = org.apache.arrow.vector.VectorSchemaRoot.create(schema, alloc)) {
                ((org.apache.arrow.vector.BigIntVector) root.getVector("id")).setSafe(0, 5);
                ((org.apache.arrow.vector.VarCharVector) root.getVector("name")).setSafe(0, "n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                ((org.apache.arrow.vector.VarCharVector) root.getVector("value")).setSafe(0, "v".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                ((org.apache.arrow.vector.VarCharVector) root.getVector("op")).setSafe(0, "insert".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                root.setRowCount(1);
                java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
                try (org.apache.arrow.vector.ipc.ArrowStreamWriter w = new org.apache.arrow.vector.ipc.ArrowStreamWriter(root, null, Channels.newChannel(out))) {
                    w.start();
                    w.writeBatch();
                    w.end();
                }
                return out.toByteArray();
            }
        }
    }
}
