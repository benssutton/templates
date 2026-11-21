package com.example.template.service;

import static com.example.template.support.SolaceSupport.SEMP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.template.config.IngestSettings;
import com.example.template.ingestion.ConnectionState;
import com.example.template.ingestion.solace.SolaceBatchConsumer;
import com.example.template.persistence.streamstore.LsmStore;
import com.example.template.support.SolaceSupport;
import com.example.template.support.SolaceSupport.Names;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Real broker; the shutdown hook stands in for System.exit(3). */
// Needs the Solace broker: the IT suffix runs it under failsafe (verify), not surefire (test).
class SolaceIngestResilienceIT {

    @Test
    // SDK keepalive takes ~30-40 s to notice a frozen broker, so the latch budget is generous.
    void watchdogRequestsShutdownWhenBrokerUnreachable() throws Exception {
        Names names = Names.unique();
        IngestSettings s = SolaceSupport.settings(names, "admin", "admin", 1, 0);
        CountDownLatch shutdown = new CountDownLatch(1);
        SolaceBatchConsumer consumer = new SolaceBatchConsumer(s);
        StreamIngestService svc =
                new StreamIngestService(
                        consumer,
                        new LsmStore(1000, 4, 1_000_000),
                        SolaceSupport.decoder(),
                        s,
                        shutdown::countDown);
        svc.start();
        try {
            await().atMost(30, TimeUnit.SECONDS).until(() -> SEMP.queueExists(names.queue()));
            // Healthy before the partition, so the shutdown below is caused by the pause.
            assertThat(consumer.connectionState()).isEqualTo(ConnectionState.CONNECTED);
            assertThat(shutdown.getCount()).as("no shutdown before pause").isEqualTo(1);
            SolaceSupport.pause();
            try {
                assertThat(shutdown.await(90, TimeUnit.SECONDS)).as("watchdog fired").isTrue();
            } finally {
                SolaceSupport.unpause();
            }
        } finally {
            svc.close();
        }
    }

    @Test
    void repeatedBindDenialRequestsShutdown() throws Exception {
        // Session CONNECTED (watchdog quiet) but every bind denied: consecutive-failure path.
        // start() throws before onBound, so nothing resets the count.
        // Real backoff 1+2+4+8 s (+ jitter) before failure 5.
        Names names = Names.unique();
        String user = "u-" + names.queue();
        SEMP.createEndpointDeniedUser("deny-" + names.queue(), user, "pw");
        IngestSettings s = SolaceSupport.settings(names, user, "pw", 60, 5);
        CountDownLatch shutdown = new CountDownLatch(1);
        StreamIngestService svc =
                new StreamIngestService(
                        new SolaceBatchConsumer(s),
                        new LsmStore(1000, 4, 1_000_000),
                        SolaceSupport.decoder(),
                        s,
                        shutdown::countDown);
        svc.start();
        try {
            assertThat(shutdown.await(60, TimeUnit.SECONDS)).as("failure shutdown fired").isTrue();
            // The consecutive-failure path ends the loop; the watchdog path would not.
            await().atMost(2, TimeUnit.SECONDS).until(() -> !svc.currentHealth().threadAlive());
        } finally {
            svc.close();
        }
    }
}
