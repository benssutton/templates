package com.example.template.flightserver;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Minimal standalone Flight server that streams synthetic batches on getStream,
 * for the docker-compose 'flight' service. Mirrors the Python example Flight server.
 * Run via: java -cp app.jar com.example.template.flightserver.ExampleFlightServer
 */
public final class ExampleFlightServer {

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getenv().getOrDefault("FLIGHT_PORT", "8815"));
        long intervalMs = Long.parseLong(System.getenv().getOrDefault("FLIGHT_INTERVAL_MS", "1000"));
        try (RootAllocator allocator = new RootAllocator();
             FlightServer server = FlightServer.builder(allocator,
                 Location.forGrpcInsecure("0.0.0.0", port), new Producer(allocator, intervalMs)).build()) {
            server.start();
            System.out.println("Flight server on " + port);
            server.awaitTermination();
        }
    }

    static final class Producer extends NoOpFlightProducer {
        private final RootAllocator allocator;
        private final long intervalMs;
        private long next = 0;

        Producer(RootAllocator allocator, long intervalMs) {
            this.allocator = allocator;
            this.intervalMs = intervalMs;
        }

        @Override
        public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
            Schema schema = new Schema(List.of(
                new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field("value", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field("op", FieldType.notNullable(new ArrowType.Utf8()), null)));
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                listener.start(root);
                for (int i = 0; i < 100; i++) {
                    long id = next++;
                    ((BigIntVector) root.getVector("id")).setSafe(0, id);
                    ((VarCharVector) root.getVector("name")).setSafe(0, ("n" + id).getBytes(StandardCharsets.UTF_8));
                    ((VarCharVector) root.getVector("value")).setSafe(0, ("v" + id).getBytes(StandardCharsets.UTF_8));
                    ((VarCharVector) root.getVector("op")).setSafe(0, "insert".getBytes(StandardCharsets.UTF_8));
                    root.setRowCount(1);
                    listener.putNext();
                    try {
                        Thread.sleep(intervalMs);
                    } catch (InterruptedException e) {
                        break;
                    }
                    root.clear();
                }
                listener.completed();
            }
        }
    }
}
