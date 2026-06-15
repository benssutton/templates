package com.example.template.ingestion.flight;

import com.example.template.config.IngestSettings;
import com.example.template.ingestion.ArrowDecoder;
import com.example.template.ingestion.BatchConsumer;
import com.example.template.ingestion.ConnectionState;
import com.example.template.persistence.streamstore.LsmRow;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * BatchConsumer backed by an Apache Arrow Flight stream. Selected when
 * {@code template.ingest.transport=flight} (the default). Mirrors the Python
 * ingestion/flight/client.py lock/close semantics:
 *
 * <p>The blocking {@code getStream()}/iteration runs OUTSIDE the lock so a
 * concurrent {@code close()} never has to wait on it. References to the live
 * client/stream are captured under a {@link ReentrantLock} and re-checked
 * against a {@code volatile boolean closing} flag, so a reconnect that races
 * close() either exits cleanly or tears down the orphan stream itself.
 */
@Singleton
@Requires(property = "template.ingest.transport", value = "flight")
public class FlightBatchConsumer implements BatchConsumer {

    private final IngestSettings settings;
    private final RootAllocator allocator = new RootAllocator();
    private final ReentrantLock lock = new ReentrantLock();

    private volatile ConnectionState state = ConnectionState.DOWN;
    private volatile boolean closing = false;
    private FlightClient client;
    private FlightStream stream;

    public FlightBatchConsumer(IngestSettings settings) {
        this.settings = settings;
    }

    @Override
    public void consume(Consumer<List<LsmRow>> sink) throws Exception {
        Location location = Location.forGrpcInsecure(
            settings.getFlight().getHost(), settings.getFlight().getPort());

        // Capture/publish the client under the lock and re-check closing, but
        // build it (cheap) and run the blocking getStream OUTSIDE the lock.
        FlightClient c;
        lock.lock();
        try {
            if (closing) {
                return;
            }
            client = FlightClient.builder(allocator, location).build();
            c = client;
            state = ConnectionState.RECONNECTING; // connected target, not yet streaming
        } finally {
            lock.unlock();
        }

        // Blocking call: raises if the server is unreachable.
        FlightStream fs = c.getStream(
            new Ticket(settings.getFlight().getTicket().getBytes(StandardCharsets.UTF_8)));

        lock.lock();
        try {
            if (closing) {            // close() won the race during getStream
                cancel(fs);
                return;
            }
            stream = fs;              // tracked so close() can cancel it
            state = ConnectionState.CONNECTED;
        } finally {
            lock.unlock();
        }

        try {
            while (fs.next()) {
                sink.accept(ArrowDecoder.decodeRoot(fs.getRoot()));
            }
        } catch (Exception e) {
            if (closing) {
                return;               // intentional shutdown — exit cleanly
            }
            state = ConnectionState.RECONNECTING;
            throw e;
        } finally {
            lock.lock();
            try {
                if (stream == fs) {   // don't clobber a stream close() swapped
                    stream = null;
                }
            } finally {
                lock.unlock();
            }
            // Release the stream's Arrow buffers on every exit path (clean end,
            // disconnect, or exception). If close() already cancelled/closed this
            // stream, a second close() is a harmless no-op.
            try {
                fs.close();
            } catch (Exception ignored) {
                // best effort
            }
        }

        if (closing) {
            return;
        }
        // Clean end of stream while not closing == an unexpected disconnect.
        state = ConnectionState.RECONNECTING;
        throw new IOException("flight stream ended unexpectedly");
    }

    private static void cancel(FlightStream s) {
        try {
            s.cancel("consumer closing", null);
        } catch (Exception ignored) {
            // best effort
        }
        try {
            s.close();
        } catch (Exception ignored) {
            // best effort
        }
    }

    @Override
    public ConnectionState connectionState() {
        return state;
    }

    @Override
    public void close() {
        // Take references under the lock so a concurrently starting consume()
        // cannot publish a stream we then fail to cancel.
        FlightStream s;
        FlightClient c;
        lock.lock();
        try {
            closing = true;
            state = ConnectionState.DOWN;
            s = stream;
            stream = null;
            c = client;
            client = null;
        } finally {
            lock.unlock();
        }
        // Cancel the in-flight stream first so a thread iterating it unblocks
        // promptly; client.close() alone may not interrupt the iteration.
        if (s != null) {
            cancel(s);
        }
        if (c != null) {
            try {
                c.close();
            } catch (Exception ignored) {
                // best effort
            }
        }
        try {
            allocator.close();
        } catch (Exception ignored) {
            // best effort — buffers may still be settling on close()
        }
    }
}
