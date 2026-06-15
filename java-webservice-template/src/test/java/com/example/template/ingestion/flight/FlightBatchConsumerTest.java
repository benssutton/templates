package com.example.template.ingestion.flight;

import com.example.template.config.IngestSettings;
import com.example.template.persistence.streamstore.LsmRow;
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
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Contract test for the Flight batch consumer. Stands up an in-process
 * {@link FlightServer} that streams exactly one batch and asserts the consumer
 * decodes it into a single {@link LsmRow}. Plain JUnit — no Micronaut, no Docker.
 */
class FlightBatchConsumerTest {

    private static final Schema SCHEMA = new Schema(List.of(
        new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
        new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null),
        new Field("value", FieldType.notNullable(new ArrowType.Utf8()), null),
        new Field("op", FieldType.notNullable(new ArrowType.Utf8()), null)));

    @Test
    void consumesSingleBatchFromEmbeddedServer() throws Exception {
        try (RootAllocator serverAllocator = new RootAllocator();
             FlightServer server = FlightServer
                 .builder(serverAllocator,
                     Location.forGrpcInsecure("localhost", 0),
                     new OneBatchProducer(serverAllocator))
                 .build()
                 .start()) {

            IngestSettings settings = new IngestSettings();
            settings.getFlight().setHost("localhost");
            settings.getFlight().setPort(server.getPort());
            settings.getFlight().setTicket("items");

            List<LsmRow> collected = new CopyOnWriteArrayList<>();
            FlightBatchConsumer consumer = new FlightBatchConsumer(settings);
            try {
                consumer.consume(collected::addAll);
            } catch (Exception expected) {
                // The server completes the stream after one batch; the consumer
                // treats a clean end while not closing as an unexpected
                // disconnect and throws so the ingest loop retries. Ignore it.
            } finally {
                consumer.close();
            }

            assertThat(collected).hasSize(1);
            LsmRow row = collected.get(0);
            assertThat(row.id()).isEqualTo(42L);
            assertThat(row.name()).isEqualTo("n");
            assertThat(row.value()).isEqualTo("v");
            assertThat(row.op()).isEqualTo("insert");
        }
    }

    /** Emits a single VectorSchemaRoot (id=42, name="n", value="v", op="insert"). */
    private static final class OneBatchProducer extends NoOpFlightProducer {

        private final RootAllocator allocator;

        OneBatchProducer(RootAllocator allocator) {
            this.allocator = allocator;
        }

        @Override
        public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
            VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator);
            try {
                BigIntVector id = (BigIntVector) root.getVector("id");
                VarCharVector name = (VarCharVector) root.getVector("name");
                VarCharVector value = (VarCharVector) root.getVector("value");
                VarCharVector op = (VarCharVector) root.getVector("op");
                id.allocateNew(1);
                name.allocateNew();
                value.allocateNew();
                op.allocateNew();
                id.set(0, 42);
                name.setSafe(0, "n".getBytes(StandardCharsets.UTF_8));
                value.setSafe(0, "v".getBytes(StandardCharsets.UTF_8));
                op.setSafe(0, "insert".getBytes(StandardCharsets.UTF_8));
                root.setRowCount(1);

                listener.start(root);
                listener.putNext();
                listener.completed();
            } catch (Exception e) {
                listener.error(e);
            } finally {
                // Close the root only after putNext/completed have serialised the
                // batch into the outbound gRPC frame; freeing the Arrow buffers any
                // earlier corrupts the frame ("CANCELLED: Failed to read message").
                root.close();
            }
        }
    }
}
