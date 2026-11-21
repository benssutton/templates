package com.example.template.controller;

import static com.example.template.support.ArrowBatches.row;
import static org.assertj.core.api.Assertions.assertThat;

import com.example.template.dto.DataRow;
import com.example.template.dto.DataRowsResponse;
import com.example.template.persistence.streamstore.LsmRow;
import com.example.template.persistence.streamstore.LsmStore;
import com.example.template.support.ArrowBatches;
import com.example.template.support.IntegrationSupport;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

@MicronautTest
class IngestHttpTest extends IntegrationSupport {

    @Inject
    @Client("/")
    HttpClient client;

    /** A small max-batch-bytes so a too-large batch is cheap to build (~100 KB here). */
    @Override
    public Map<String, String> getProperties() {
        Map<String, String> props = super.getProperties();
        props.put("template.ingest.max-batch-bytes", "65536");
        return props;
    }

    /** One-row IPC stream with the given fields; a null cell value leaves that cell null. */
    private static byte[] ipc(List<Field> fields, Object... cells) throws Exception {
        try (RootAllocator alloc = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), alloc)) {
            for (int i = 0; i < fields.size(); i++) {
                Object cell = cells[i];
                if (cell == null) {
                    root.getVector(i).setNull(0);
                } else if (root.getVector(i) instanceof BigIntVector v) {
                    v.setSafe(0, (Long) cell);
                } else {
                    ((VarCharVector) root.getVector(i))
                            .setSafe(0, cell.toString().getBytes(StandardCharsets.UTF_8));
                }
            }
            root.setRowCount(1);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowStreamWriter w =
                    new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
                w.start();
                w.writeBatch();
                w.end();
            }
            return out.toByteArray();
        }
    }

    private static Field utf8(String name) {
        return new Field(name, FieldType.nullable(new ArrowType.Utf8()), null);
    }

    private static Field int64(String name) {
        return new Field(name, FieldType.nullable(new ArrowType.Int(64, true)), null);
    }

    /** Status and body of a POST that may fail. Object: HttpStatus is a CharSequence. */
    private Object[] postIngest(byte[] body) {
        try {
            var res =
                    client.toBlocking()
                            .exchange(
                                    HttpRequest.POST("/data/ingest", body)
                                            .contentType(MediaType.APPLICATION_OCTET_STREAM),
                                    String.class);
            return new Object[] {res.getStatus(), res.getBody().orElse("")};
        } catch (HttpClientResponseException e) {
            return new Object[] {e.getStatus(), e.getResponse().getBody(String.class).orElse("")};
        }
    }

    @Test
    void nonArrowBodyIs400WithDetail() {
        Object[] res = postIngest("not arrow ipc".getBytes(StandardCharsets.UTF_8));
        assertThat(res[0]).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat((String) res[1]).contains("\"detail\"");
    }

    @Test
    void missingOpColumnIs422() throws Exception {
        Object[] res =
                postIngest(ipc(List.of(int64("id"), utf8("name"), utf8("value")), 1L, "n", "v"));
        assertThat(res[0]).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat((String) res[1]).contains("op");
    }

    @Test
    void stringIdColumnIs422() throws Exception {
        Object[] res =
                postIngest(
                        ipc(
                                List.of(utf8("id"), utf8("name"), utf8("value"), utf8("op")),
                                "1",
                                "n",
                                "v",
                                "upsert"));
        assertThat(res[0]).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat((String) res[1]).contains("id");
    }

    @Test
    void nullKeyCellIs422() throws Exception {
        Object[] res =
                postIngest(
                        ipc(
                                List.of(int64("id"), utf8("name"), utf8("value"), utf8("op")),
                                null,
                                "n",
                                "v",
                                "upsert"));
        assertThat(res[0]).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
    }

    @Test
    void nullNameAndValueAreAccepted() throws Exception {
        Object[] res =
                postIngest(
                        ipc(
                                List.of(int64("id"), utf8("name"), utf8("value"), utf8("op")),
                                99L,
                                null,
                                null,
                                "upsert"));
        assertThat(res[0]).isEqualTo(HttpStatus.ACCEPTED);
    }

    @Test
    void ingestAcceptsArrowAndCacheReturnsTheMergedRow() throws Exception {
        HttpStatus status =
                client.toBlocking()
                        .exchange(
                                HttpRequest.POST(
                                                "/data/ingest",
                                                ArrowBatches.encode(List.of(row(7, "upsert"))))
                                        .contentType(MediaType.APPLICATION_OCTET_STREAM))
                        .getStatus();
        assertThat((Object) status).isEqualTo(HttpStatus.ACCEPTED);

        // The store is shared by this class's tests, so look for this test's row (id 7).
        DataRowsResponse r =
                client.toBlocking()
                        .retrieve(HttpRequest.GET("/data/cache?limit=100"), DataRowsResponse.class);
        assertThat(r.rows()).filteredOn(r2 -> r2.id() == 7L).hasSize(1);
        assertThat(r.rows()).extracting(DataRow::id).isSorted();
    }

    /** Newest version per key wins and a delete removes the key; rows carry no seqno or op. */
    @Test
    void cacheServesTheMergedView() {
        client.toBlocking()
                .exchange(
                        HttpRequest.POST(
                                        "/data/ingest",
                                        ArrowBatches.encodeBatches(
                                                List.of(
                                                        List.of(
                                                                new LsmRow(
                                                                        -3, "n", "old", 0,
                                                                        "upsert"),
                                                                row(-2, "upsert")),
                                                        List.of(
                                                                new LsmRow(
                                                                        -3, "n", "new", 0,
                                                                        "upsert"),
                                                                row(-2, "delete")))))
                                .contentType(MediaType.APPLICATION_OCTET_STREAM));
        String body = client.toBlocking().retrieve(HttpRequest.GET("/data/cache?limit=100"));
        assertThat(body).doesNotContain("seqno").doesNotContain("\"op\"");
        DataRowsResponse r =
                client.toBlocking()
                        .retrieve(HttpRequest.GET("/data/cache?limit=100"), DataRowsResponse.class);
        assertThat(r.rows()).extracting(DataRow::id).contains(-3L).doesNotContain(-2L);
        assertThat(r.rows())
                .filteredOn(row -> row.id() == -3L)
                .extracting(DataRow::value)
                .containsExactly("new");
    }

    @Inject LsmStore store;

    private double outcomeCount(String outcome) {
        String body = client.toBlocking().retrieve(HttpRequest.GET("/metrics"));
        String prefix = "ingest_outcomes_total{outcome=\"" + outcome + "\"} ";
        return body.lines()
                .filter(l -> l.startsWith(prefix))
                .mapToDouble(l -> Double.parseDouble(l.substring(prefix.length())))
                .findFirst()
                .orElseThrow();
    }

    /** HTTP ingest counts in ingest_outcomes_total, like a transport delivery. */
    @Test
    void httpIngestIsCountedByOutcome() {
        double accepted = outcomeCount("accepted");
        double rejected = outcomeCount("rejected");
        assertThat(postIngest(ArrowBatches.encode(List.of(row(601, "upsert"))))[0])
                .isEqualTo(HttpStatus.ACCEPTED);
        assertThat(postIngest(ArrowBatches.encode(List.of(row(602, "bogus"))))[0])
                .isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(outcomeCount("accepted")).isEqualTo(accepted + 1);
        assertThat(outcomeCount("rejected")).isEqualTo(rejected + 1);
    }

    /** A batch over max-batch-bytes is refused by the service, so it counts as rejected. */
    @Test
    void tooLargeBatchIsCountedAsRejected() {
        double rejected = outcomeCount("rejected");
        List<LsmRow> rows = new ArrayList<>();
        String big = "x".repeat(100);
        for (long id = 1_000; id < 2_000; id++) {
            rows.add(new LsmRow(id, "n", big, 0, "upsert"));
        }
        assertThat(postIngest(ArrowBatches.encode(rows))[0])
                .isEqualTo(HttpStatus.REQUEST_ENTITY_TOO_LARGE);
        assertThat(outcomeCount("rejected")).isEqualTo(rejected + 1);
    }

    /** An unreadable body is refused before the service decodes a batch: not counted. */
    @Test
    void unreadableBodyIsNotCountedAsRejected() {
        double rejected = outcomeCount("rejected");
        assertThat(postIngest("not arrow ipc".getBytes(StandardCharsets.UTF_8))[0])
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(outcomeCount("rejected")).isEqualTo(rejected);
    }

    /** A stream whose second batch is invalid stores nothing from its first batch either. */
    @Test
    void everyBatchIsValidatedBeforeAnyIsWritten() {
        long before = store.query(1).total();
        byte[] body =
                ArrowBatches.encodeBatches(
                        List.of(List.of(row(501, "upsert")), List.of(row(502, "not-an-op"))));
        Object[] res = postIngest(body);
        assertThat(res[0]).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(store.query(1).total()).isEqualTo(before);
    }

    @Test
    void unknownOpIs422() {
        Object[] res = postIngest(ArrowBatches.encode(List.of(row(1, "insert"))));
        assertThat(res[0]).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat((String) res[1]).contains("op");
    }
}
