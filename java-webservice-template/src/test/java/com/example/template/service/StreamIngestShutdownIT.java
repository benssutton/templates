package com.example.template.service;

import static com.example.template.support.SolaceSupport.SEMP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.template.support.IntegrationSupport;
import com.example.template.support.SolaceSupport;
import com.example.template.support.SolaceSupport.Names;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.Environment;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Closing the application context stops ingest: the ingest thread ends and the Solace flow and
 * session are torn down, as seen from the broker (SEMP). Plain JUnit (not @MicronautTest) so the
 * test owns the context's lifecycle.
 */
// Needs the Solace broker: the IT suffix runs it under failsafe (verify), not surefire (test).
class StreamIngestShutdownIT extends IntegrationSupport {

    @Test
    void closingTheContextUnbindsTheQueueAndDisconnects() {
        Names names = Names.unique();
        Map<String, String> props = getProperties();
        props.put("template.ingest.transport", "solace");
        props.put("template.ingest.solace.host", SolaceSupport.SOLACE.getHost());
        props.put(
                "template.ingest.solace.port",
                String.valueOf(SolaceSupport.SOLACE.getMappedPort(55555)));
        props.put("template.ingest.solace.topic", names.topic());
        props.put("template.ingest.solace.queue", names.queue());
        String clientName =
                names.queue(); // SolaceBatchConsumer.clientName(): queue with '/' -> '_'

        ApplicationContext context = ApplicationContext.run(Map.copyOf(props), Environment.TEST);
        try {
            await().atMost(30, TimeUnit.SECONDS)
                    .until(() ->
                            SEMP.queueExists(names.queue())
                                    && SEMP.queueBindCount(names.queue()) == 1);
            assertThat(SEMP.clientCount(clientName)).as("connected clients").isEqualTo(1);
            StreamIngestService svc = context.getBean(StreamIngestService.class);
            assertThat(svc.currentHealth().threadAlive()).isTrue();

            context.close();

            assertThat(svc.currentHealth().threadAlive())
                    .as("ingest thread alive after close")
                    .isFalse();
            // close() has returned, so only the broker's own bookkeeping can lag.
            await().atMost(5, TimeUnit.SECONDS)
                    .until(() ->
                            SEMP.queueBindCount(names.queue()) == 0
                                    && SEMP.clientCount(clientName) == 0);
        } finally {
            if (context.isRunning()) {
                context.close();
            }
        }
    }
}
