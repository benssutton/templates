package com.example.template.controller;

import com.example.template.dto.CachedDataRowsResponse;
import com.example.template.support.IntegrationSupport;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
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

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
class IngestHttpTest extends IntegrationSupport {

    @Inject
    @Client("/")
    HttpClient client;

    private byte[] arrowBatch() throws Exception {
        try (RootAllocator alloc = new RootAllocator()) {
            Schema schema = new Schema(List.of(
                new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field("value", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field("op", FieldType.notNullable(new ArrowType.Utf8()), null)));
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
                ((BigIntVector) root.getVector("id")).setSafe(0, 7);
                ((VarCharVector) root.getVector("name")).setSafe(0, "n".getBytes(StandardCharsets.UTF_8));
                ((VarCharVector) root.getVector("value")).setSafe(0, "v".getBytes(StandardCharsets.UTF_8));
                ((VarCharVector) root.getVector("op")).setSafe(0, "insert".getBytes(StandardCharsets.UTF_8));
                root.setRowCount(1);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                try (ArrowStreamWriter w = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
                    w.start();
                    w.writeBatch();
                    w.end();
                }
                return out.toByteArray();
            }
        }
    }

    @Test
    void ingestAcceptsArrowAndCacheReturnsRowWithSeqnoAndOp() throws Exception {
        HttpStatus status = client.toBlocking().exchange(
            HttpRequest.POST("/data/ingest", arrowBatch()).contentType(MediaType.APPLICATION_OCTET_STREAM))
            .getStatus();
        assertThat((Object) status).isEqualTo(HttpStatus.ACCEPTED);

        CachedDataRowsResponse r = client.toBlocking().retrieve(
            HttpRequest.GET("/data/cache?limit=10"), CachedDataRowsResponse.class);
        assertThat(r.total()).isEqualTo(1);
        assertThat(r.rows().get(0).id()).isEqualTo(7L);
        assertThat(r.rows().get(0).op()).isEqualTo("insert");
        assertThat(r.rows().get(0).seqno()).isEqualTo(0L);
    }
}
