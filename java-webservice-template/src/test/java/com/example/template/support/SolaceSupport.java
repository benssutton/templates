package com.example.template.support;

import com.example.template.config.IngestSettings;
import com.github.dockerjava.api.model.Ulimit;
import com.solace.messaging.MessagingService;
import com.solace.messaging.config.profile.ConfigurationProfile;
import com.solace.messaging.publisher.PersistentMessagePublisher;
import com.solace.messaging.resources.Topic;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * One Solace broker per JVM (POST + start is ~60 s); tests isolate by unique
 * topic/queue names. Configured exactly like the Python fixture.
 */
public final class SolaceSupport {

    public static final GenericContainer<?> SOLACE =
        new GenericContainer<>(DockerImageName.parse("solace/solace-pubsub-standard:10.26.10"))
            .withExposedPorts(55555, 8080, 5550)
            .withEnv("username_admin_globalaccesslevel", "admin")
            .withEnv("username_admin_password", "admin")
            .withEnv("system_scaling_maxconnectioncount", "100")
            .withSharedMemorySize(1024L * 1024 * 1024)
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withUlimits(List.of(
                new Ulimit("core", -1L, -1L), new Ulimit("nofile", 1048576L, 1048576L))))
            // Readiness isn't logged to stdout; the guaranteed-messaging health port is authoritative.
            .waitingFor(Wait.forHttp("/health-check/guaranteed-active").forPort(5550)
                .forStatusCode(200).withStartupTimeout(Duration.ofMinutes(3)));

    public static final Semp SEMP;

    static {
        SOLACE.start();
        awaitMessaging();
        SEMP = new Semp("http://" + SOLACE.getHost() + ":" + SOLACE.getMappedPort(8080));
    }

    private SolaceSupport() {}

    /**
     * The health endpoint reports active before the message VPN accepts SMF clients
     * ("Not ready"), so wait until a real connect succeeds (0.5 s doubling to 5 s, 120 s budget).
     */
    private static void awaitMessaging() {
        long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
        long delayMs = 500;
        while (true) {
            try {
                MessagingService svc = MessagingService.builder(ConfigurationProfile.V1)
                    .fromProperties(connectionProps()).build().connect();
                svc.disconnect();
                return;
            } catch (RuntimeException e) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("Solace messaging not ready within 120s", e);
                }
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(ie);
                }
                delayMs = Math.min(delayMs * 2, 5000);
            }
        }
    }

    private static Properties connectionProps() {
        Properties props = new Properties();
        props.setProperty("solace.messaging.transport.host", "tcp://" + SOLACE.getHost() + ":" + SOLACE.getMappedPort(55555));
        props.setProperty("solace.messaging.service.vpn-name", "default");
        props.setProperty("solace.messaging.authentication.basic.username", "admin");
        props.setProperty("solace.messaging.authentication.basic.password", "admin");
        return props;
    }

    public record Names(String topic, String queue) {
        public static Names unique() {
            String uid = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
            return new Names("test/" + uid + "/batches", "test-" + uid);
        }
    }

    public static IngestSettings settings(Names names) {
        IngestSettings s = new IngestSettings();
        s.setTransport("solace");
        IngestSettings.Solace sol = s.getSolace();
        sol.setHost(SOLACE.getHost());
        sol.setPort(SOLACE.getMappedPort(55555));
        sol.setVpn("default");
        sol.setUsername("admin");
        sol.setPassword("admin");
        sol.setTopic(names.topic());
        sol.setQueue(names.queue());
        return s;
    }

    /** Guaranteed publish, waiting for the broker ack (spooled on matching queues). */
    public static void publish(String topic, byte[] payload) throws Exception {
        MessagingService service = MessagingService.builder(ConfigurationProfile.V1).fromProperties(connectionProps()).build().connect();
        try {
            PersistentMessagePublisher publisher = service.createPersistentMessagePublisherBuilder().build().start();
            try {
                publisher.publishAwaitAcknowledgement(service.messageBuilder().build(payload), Topic.of(topic), 5_000L);
            } finally {
                publisher.terminate(5_000);
            }
        } finally {
            service.disconnect();
        }
    }

    /** Freeze the broker: a genuine network partition for connected clients. */
    public static void pause() {
        DockerClientFactory.instance().client().pauseContainerCmd(SOLACE.getContainerId()).exec();
    }

    public static void unpause() {
        DockerClientFactory.instance().client().unpauseContainerCmd(SOLACE.getContainerId()).exec();
    }
}
