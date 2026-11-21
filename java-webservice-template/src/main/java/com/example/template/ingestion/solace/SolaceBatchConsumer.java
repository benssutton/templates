package com.example.template.ingestion.solace;

import com.example.template.config.IngestSettings;
import com.example.template.ingestion.BatchConsumer;
import com.example.template.ingestion.ConnectionState;
import com.example.template.ingestion.Delivery;
import com.solace.messaging.MessagingService;
import com.solace.messaging.config.MessageAcknowledgementConfiguration.Outcome;
import com.solace.messaging.config.MissingResourcesCreationConfiguration.MissingResourcesCreationStrategy;
import com.solace.messaging.config.RetryStrategy;
import com.solace.messaging.config.profile.ConfigurationProfile;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import com.solace.messaging.resources.Queue;
import com.solace.messaging.resources.TopicSubscription;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Solace Guaranteed messaging. Selected when {@code template.ingest.transport=solace}. Mirrors the
 * Python ingestion/solace/client.py: binds a durable exclusive queue (created on start, with the
 * topic subscription attached) and hands one {@link Delivery} per message to the sink;
 * StreamIngestService settles it after the store write. SDK lifecycle listeners drive {@link
 * ConnectionState}.
 *
 * <p>Threading: {@link #consume} runs on the ingest thread and owns one session (connection +
 * receiver + inbox) per call, torn down in {@code finally}, so a retry after a failure always
 * reconnects from scratch. {@link #close} runs on another thread: it sets {@code closed}, wakes the
 * consume thread and terminates whatever session is currently published. Both sides tear down
 * best-effort, so a race at most terminates the same resource twice, harmlessly.
 */
@Singleton
@Requires(property = "template.ingest.transport", value = "solace")
public class SolaceBatchConsumer implements BatchConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(SolaceBatchConsumer.class);

    /** SDK reconnects forever; the ingest watchdog is the single place that gives up. */
    private static final int RECONNECT_INTERVAL_MS = 1000;

    /** Identity-compared sentinels placed on the inbox to wake a blocked consume(). */
    private static final Delivery CLOSED = new SolaceDelivery(null, null, new byte[0]);

    private static final Delivery INTERRUPTED = new SolaceDelivery(null, null, new byte[0]);

    private final IngestSettings settings;
    private volatile boolean closed;
    private volatile ConnectionState state = ConnectionState.DOWN;
    // The current consume() session, published for close(); null between sessions.
    private volatile BlockingQueue<Delivery> inbox;
    private volatile MessagingService service;
    private volatile PersistentMessageReceiver receiver;

    public SolaceBatchConsumer(IngestSettings settings) {
        this.settings = settings;
    }

    /** Deterministic per-replica client name (one queue per replica, so one name). */
    public String clientName() {
        return settings.solace().queue().replace('/', '_');
    }

    private MessagingService connect(BlockingQueue<Delivery> wake) {
        IngestSettings.Solace s = settings.solace();
        Properties props = new Properties();
        props.setProperty("solace.messaging.transport.host", "tcp://" + s.host() + ":" + s.port());
        props.setProperty("solace.messaging.service.vpn-name", s.vpn());
        props.setProperty("solace.messaging.authentication.basic.username", s.username());
        props.setProperty("solace.messaging.authentication.basic.password", s.password());
        MessagingService svc =
                MessagingService.builder(ConfigurationProfile.V1)
                        .fromProperties(props)
                        .withReconnectionRetryStrategy(
                                RetryStrategy.foreverRetry(RECONNECT_INTERVAL_MS))
                        .build(clientName())
                        .connect();
        service = svc; // published for close() and for the listeners' session check below
        state = ConnectionState.CONNECTED;
        svc.addReconnectionAttemptListener(
                e -> setStateIfCurrent(svc, ConnectionState.RECONNECTING));
        svc.addReconnectionListener(e -> setStateIfCurrent(svc, ConnectionState.CONNECTED));
        svc.addServiceInterruptionListener(e -> {
            // Terminal: this service will never recover. Make consume() fail so the
            // ingest loop backs off and the next call reconnects with a fresh service.
            setStateIfCurrent(svc, ConnectionState.DOWN);
            wake.offer(INTERRUPTED);
        });
        LOG.info(
                "Solace: connected to {} vpn={} as {}",
                props.getProperty("solace.messaging.transport.host"),
                s.vpn(),
                clientName());
        return svc;
    }

    /**
     * Ignore late lifecycle events from a torn-down service so they can't overwrite a newer
     * session's state.
     */
    private void setStateIfCurrent(MessagingService svc, ConnectionState newState) {
        if (service == svc) {
            state = newState;
        }
    }

    @Override
    public void consume(Consumer<Delivery> sink) throws Exception {
        consume(sink, () -> {});
    }

    /** {@code onBound} runs on this (the ingest) thread once the receiver has started. */
    @Override
    public void consume(Consumer<Delivery> sink, Runnable onBound) throws Exception {
        if (closed) {
            return;
        }
        // Unbounded type, but effectively bounded: the broker stops delivering once the
        // flow's unacked window is full, and nothing is acked until the sink settles it.
        BlockingQueue<Delivery> box = new LinkedBlockingQueue<>();
        MessagingService svc = null;
        PersistentMessageReceiver r = null;
        inbox = box;
        try {
            // Publish each resource (connect() publishes the service) before re-checking
            // `closed`: either close() sees it and terminates it, or we see `closed` and
            // the finally block does.
            svc = connect(box); // lazy: the ingest loop's backoff wraps consume(), including
            // connect
            if (closed) {
                return;
            }
            IngestSettings.Solace s = settings.solace();
            r =
                    svc.createPersistentMessageReceiverBuilder()
                            .withMissingResourcesCreationStrategy(
                                    MissingResourcesCreationStrategy.CREATE_ON_START)
                            .withMessageClientAcknowledgement()
                            .withRequiredMessageClientOutcomeOperationSupport(
                                    Outcome.FAILED, Outcome.REJECTED)
                            .withSubscriptions(
                                    TopicSubscription.of(s.topic())) // added to the queue on start
                            .build(Queue.durableExclusiveQueue(s.queue()))
                            .start(); // throws if the broker denies queue creation/bind
            receiver = r;
            if (closed) {
                return;
            }
            PersistentMessageReceiver bound =
                    r; // the callback settles on this flow, never a later one
            bound.receiveAsync(m -> onMessage(bound, box, m));
            LOG.info("Solace: bound queue {} (subscription {})", s.queue(), s.topic());
            onBound.run(); // connect + bind succeeded: the transport is healthy

            while (true) {
                Delivery d = box.take();
                if (d == CLOSED || closed) {
                    // CLOSED queues behind buffered deliveries; don't hand those to the
                    // sink once closed (their flow is terminated). The broker redelivers them.
                    return;
                }
                if (d == INTERRUPTED) {
                    throw new IllegalStateException("Solace: service terminally interrupted");
                }
                sink.accept(d);
            }
        } catch (Exception e) {
            if (closed) {
                return; // close() tore the session down under us: a clean stop, not a failure
            }
            throw e;
        } finally {
            inbox = null;
            receiver = null;
            service = null;
            teardown(r, svc);
            state = ConnectionState.DOWN;
        }
    }

    /**
     * Hands the raw payload on: StreamIngestService checks its size before decoding, so an
     * oversized message is rejected without spending decoding memory on it.
     */
    private void onMessage(
            PersistentMessageReceiver r, BlockingQueue<Delivery> box, InboundMessage message) {
        byte[] payload = message.getPayloadAsBytes();
        box.offer(new SolaceDelivery(r, message, payload == null ? new byte[0] : payload));
    }

    /**
     * Best effort and safe to repeat. {@code terminate(0)} drops in-flight deliveries without
     * settling them, which is safe: unsettled messages stay on the durable queue and the broker
     * redelivers them on the next bind.
     */
    private static void teardown(PersistentMessageReceiver r, MessagingService svc) {
        if (r != null) {
            try {
                r.terminate(0);
            } catch (Exception ignored) {
                // already terminated, or the connection is gone
            }
        }
        if (svc != null) {
            try {
                svc.disconnect();
            } catch (Exception ignored) {
                // already disconnected
            }
        }
    }

    @Override
    public ConnectionState connectionState() {
        return state;
    }

    @Override
    public void close() {
        closed = true;
        state = ConnectionState.DOWN;
        BlockingQueue<Delivery> box = inbox;
        if (box != null) {
            box.offer(CLOSED);
        }
        // consume()'s finally also tears down; terminating here too unblocks it if it
        // is stuck in the SDK (connect/start) or the sink, and covers shutdown timeouts.
        teardown(receiver, service);
    }

    private record SolaceDelivery(
            PersistentMessageReceiver receiver, InboundMessage message, byte[] payload)
            implements Delivery {
        @Override
        public String correlationId() {
            return message.getCorrelationId();
        }

        @Override
        public boolean redelivered() {
            return message.isRedelivered();
        }

        @Override
        public void accept() {
            receiver.ack(message);
        }

        @Override
        public void fail() {
            receiver.settle(message, Outcome.FAILED);
        }

        @Override
        public void reject() {
            receiver.settle(message, Outcome.REJECTED);
        }
    }
}
