package com.example.template.ingestion.solace;

import com.example.template.config.IngestSettings;
import com.example.template.ingestion.ArrowDecoder;
import com.example.template.ingestion.BatchConsumer;
import com.example.template.ingestion.ConnectionState;
import com.example.template.persistence.streamstore.LsmRow;
import com.solace.messaging.MessagingService;
import com.solace.messaging.config.profile.ConfigurationProfile;
import com.solace.messaging.receiver.DirectMessageReceiver;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.resources.TopicSubscription;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

/**
 * {@link BatchConsumer} backed by Solace PubSub+ direct messaging. Selected when
 * {@code template.ingest.transport=solace}. Mirrors the Python
 * ingestion/solace/client.py: it connects a {@link MessagingService} (basic
 * auth from {@link IngestSettings}), subscribes a {@link DirectMessageReceiver}
 * to the configured topic, and decodes each inbound Arrow IPC payload into a
 * {@code List<LsmRow>} which is handed to the {@code consume()} sink via a
 * {@link BlockingQueue}.
 *
 * <p>The async Solace handler thread decodes and enqueues; the {@code consume()}
 * caller thread drains. {@code close()} enqueues an empty-list sentinel so a
 * blocked {@code queue.take()} unblocks and {@code consume()} returns cleanly.
 */
@Singleton
@Requires(property = "template.ingest.transport", value = "solace")
public class SolaceBatchConsumer implements BatchConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(SolaceBatchConsumer.class);
    /** Identity-comparable empty batch used to wake a blocked consume() on close. */
    private static final List<LsmRow> SENTINEL = List.of();

    private final IngestSettings settings;
    private final BlockingQueue<List<LsmRow>> queue = new LinkedBlockingQueue<>();
    private volatile ConnectionState state = ConnectionState.DOWN;
    private MessagingService service;
    private DirectMessageReceiver receiver;

    public SolaceBatchConsumer(IngestSettings settings) {
        this.settings = settings;
    }

    private void connect() {
        IngestSettings.Solace s = settings.getSolace();
        Properties props = new Properties();
        props.setProperty("solace.messaging.transport.host",
            "tcp://" + s.getHost() + ":" + s.getPort());
        props.setProperty("solace.messaging.service.vpn-name", s.getVpn());
        props.setProperty("solace.messaging.authentication.basic.username", s.getUsername());
        props.setProperty("solace.messaging.authentication.basic.password", s.getPassword());
        service = MessagingService.builder(ConfigurationProfile.V1)
            .fromProperties(props)
            .build()
            .connect();
        state = ConnectionState.CONNECTED;
        LOG.info("Solace: connected to {} vpn={}", props.getProperty("solace.messaging.transport.host"), s.getVpn());
    }

    @Override
    public void consume(Consumer<List<LsmRow>> sink) throws Exception {
        if (service == null) {
            connect();
        }
        receiver = service.createDirectMessageReceiverBuilder()
            .withSubscriptions(TopicSubscription.of(settings.getSolace().getTopic()))
            .build()
            .start();
        receiver.receiveAsync(this::onMessage);
        LOG.info("Solace: subscribed to topic {}", settings.getSolace().getTopic());

        while (true) {
            List<LsmRow> batch = queue.take();
            if (batch == SENTINEL) {
                return;
            }
            sink.accept(batch);
        }
    }

    /** Async Solace handler: decode the Arrow IPC payload and enqueue the rows. */
    private void onMessage(InboundMessage message) {
        try (ArrowDecoder decoder = new ArrowDecoder()) {
            queue.put(decoder.decodeAll(message.getPayloadAsBytes()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.warn("Solace: malformed IPC message dropped", e);
        }
    }

    @Override
    public ConnectionState connectionState() {
        return state;
    }

    @Override
    public void close() {
        state = ConnectionState.DOWN;
        queue.offer(SENTINEL);
        if (receiver != null) {
            try {
                receiver.terminate(0);
            } catch (Exception ignored) {
                // best effort
            }
            receiver = null;
        }
        if (service != null) {
            try {
                service.disconnect();
            } catch (Exception ignored) {
                // best effort
            }
            service = null;
        }
    }
}
