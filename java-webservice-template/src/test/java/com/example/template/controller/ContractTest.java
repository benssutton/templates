package com.example.template.controller;

import static com.example.template.support.ArrowBatches.row;
import static org.assertj.core.api.Assertions.assertThat;

import com.example.template.support.ArrowBatches;
import com.example.template.support.IntegrationSupport;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.json.JsonMapper;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.RedisClient;

/**
 * The wire contract shared with the Python and Rust templates, asserted on raw response text (never
 * read back through this service's own DTOs), so a field renamed by a refactor or a different
 * naming strategy fails here. Bodies with variable values are parsed into plain maps to check their
 * exact key sets.
 */
@MicronautTest
class ContractTest extends IntegrationSupport {

    private static final String ERROR = "\\{\"detail\":\".+\"}";
    private static final String ARROW_STREAM = "application/vnd.apache.arrow.stream";

    @Inject
    @Client("/")
    HttpClient client;

    @Inject JsonMapper json;

    @Inject RedisClient jedis;

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> props = super.getProperties();
        props.put("template.ingest.max-batch-bytes", "4096");
        props.put("micronaut.server.max-request-size", "1MB");
        return props;
    }

    record Res(int status, String contentType, String body) {}

    private Res send(MutableHttpRequest<?> request) {
        try {
            HttpResponse<String> r = client.toBlocking().exchange(request, String.class);
            return new Res(
                    r.code(),
                    r.getContentType().map(MediaType::toString).orElse(""),
                    r.getBody().orElse(""));
        } catch (HttpClientResponseException e) {
            return new Res(
                    e.code(),
                    e.getResponse().getContentType().map(MediaType::toString).orElse(""),
                    e.getResponse().getBody(String.class).orElse(""));
        }
    }

    private Map<String, Object> object(String body) throws Exception {
        @SuppressWarnings("unchecked")
        Map<String, Object> map = json.readValue(body, Map.class);
        return map;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> child(Map<String, Object> parent, String key) {
        return (Map<String, Object>) parent.get(key);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> list(Map<String, Object> parent, String key) {
        return (List<Map<String, Object>>) parent.get(key);
    }

    private void assertError(Res res, int status) {
        assertThat(res.status()).as(res.body()).isEqualTo(status);
        assertThat(res.contentType())
                .as("content type of %s", status)
                .startsWith("application/json");
        assertThat(res.body()).as("body of %s", status).matches(ERROR);
    }

    // ---- success shapes ----

    @Test
    void rootKeepsTheUpperCaseMcpKey() {
        Res res = send(HttpRequest.GET("/"));
        assertThat(res.body())
                .isEqualTo(
                        "{\"title\":\"Template Micronaut Service\",\"version\":\"1.0.0\","
                                + "\"description\":\"A Java Micronaut service mirroring the Python webservice template\","
                                + "\"docs\":\"/swagger-ui\",\"MCP\":\"/mcp\"}");
    }

    @Test
    void configShapes() {
        Res created =
                send(
                        HttpRequest.POST("/config", "{\"key\":\"contract-k\",\"value\":\"v\"}")
                                .contentType(MediaType.APPLICATION_JSON));
        assertThat(created.status()).isEqualTo(201);
        assertThat(created.body()).isEqualTo("{\"key\":\"contract-k\",\"value\":\"v\"}");
        assertThat(send(HttpRequest.GET("/config")).body())
                .contains("{\"key\":\"contract-k\",\"value\":\"v\"}");
    }

    @Test
    void cacheShapesAreSnakeCaseWithExplicitNulls() {
        Res withTtl =
                send(
                        HttpRequest.POST(
                                        "/cache",
                                        "{\"key\":\"contract-c1\",\"value\":{\"a\":1},\"ttl_seconds\":60}")
                                .contentType(MediaType.APPLICATION_JSON));
        assertThat(withTtl.status()).isEqualTo(201);
        assertThat(withTtl.body())
                .isEqualTo("{\"key\":\"contract-c1\",\"value\":{\"a\":1},\"ttl_seconds\":60}");

        Res noTtl =
                send(
                        HttpRequest.POST("/cache", "{\"key\":\"contract-c2\",\"value\":\"s\"}")
                                .contentType(MediaType.APPLICATION_JSON));
        assertThat(noTtl.body())
                .isEqualTo("{\"key\":\"contract-c2\",\"value\":\"s\",\"ttl_seconds\":null}");

        assertThat(send(HttpRequest.GET("/cache/contract-c2")).body())
                .isEqualTo("{\"key\":\"contract-c2\",\"value\":\"s\",\"ttl_seconds\":null}");
        assertThat(send(HttpRequest.GET("/cache/contract-c1")).body())
                .matches(
                        "\\{\"key\":\"contract-c1\",\"value\":\\{\"a\":1},\"ttl_seconds\":([1-5]?[0-9]|60)}");
    }

    @Test
    void dataShapes() throws Exception {
        Map<String, Object> data = object(send(HttpRequest.GET("/data?limit=2")).body());
        assertThat(data).containsOnlyKeys("rows", "total", "limit");
        assertThat(data.get("rows")).isInstanceOf(List.class);

        for (String type : List.of(ARROW_STREAM, MediaType.APPLICATION_OCTET_STREAM)) {
            Res accepted =
                    send(
                            HttpRequest.POST(
                                            "/data/ingest",
                                            ArrowBatches.encode(List.of(row(41, "upsert"))))
                                    .contentType(type));
            assertThat(accepted.status()).as(type).isEqualTo(202);
            assertThat(accepted.body()).as(type).isEqualTo("{\"accepted\":true}");
        }
        Map<String, Object> cached = object(send(HttpRequest.GET("/data/cache?limit=1")).body());
        assertThat(cached).containsOnlyKeys("rows", "total", "limit");
        assertThat(list(cached, "rows").get(0))
                .containsOnlyKeys("id", "name", "value", "seqno", "op");
    }

    @Test
    void healthShapes() throws Exception {
        assertThat(object(send(HttpRequest.GET("/health/live")).body()))
                .containsOnlyKeys("status", "uptime_seconds");

        Res readyRes = send(HttpRequest.GET("/health/ready"));
        assertThat(readyRes.status()).as(readyRes.body()).isEqualTo(200);
        Map<String, Object> ready = object(readyRes.body());
        assertThat(ready).containsOnlyKeys("status", "checks");
        assertThat(ready.get("status")).isEqualTo("ready");
        assertThat(list(ready, "checks"))
                .allSatisfy(c ->
                        assertThat(c)
                                .containsOnlyKeys(
                                        "name",
                                        "status",
                                        "latency_ms",
                                        "transport",
                                        "connection_state",
                                        "thread_alive",
                                        "last_batch_at",
                                        "seconds_since_last_batch",
                                        "error"));

        Map<String, Object> status = object(send(HttpRequest.GET("/health/status")).body());
        assertThat(status)
                .containsOnlyKeys("app", "uptime", "dependencies", "ingest", "requests", "system");
        assertThat(child(status, "app")).containsOnlyKeys("title", "version", "status");
        assertThat(child(status, "uptime"))
                .containsOnlyKeys("process_seconds", "system_boot_seconds");
        assertThat(list(status, "dependencies"))
                .extracting(d -> d.get("name"))
                .containsExactlyInAnyOrder("postgres", "clickhouse", "redis");
        assertThat(list(status, "dependencies"))
                .allSatisfy(d ->
                        assertThat(d).containsOnlyKeys("name", "status", "latency_ms", "error"));
        assertThat(child(status, "ingest"))
                .containsOnlyKeys(
                        "transport",
                        "connection_state",
                        "thread_alive",
                        "last_batch_at",
                        "seconds_since_last_batch",
                        "rows_ingested_total",
                        "rows_evicted_total",
                        "stale");
        assertThat(child(status, "requests")).containsOnlyKeys("last_request_at");
        assertThat(child(child(status, "system"), "process"))
                .containsOnlyKeys("cpu_percent", "memory_rss_bytes", "num_threads", "open_files");
        assertThat(child(child(status, "system"), "host"))
                .containsOnlyKeys(
                        "cpu_percent",
                        "memory_total_bytes",
                        "memory_available_bytes",
                        "memory_percent");
    }

    // ---- error shapes: every status is application/json {"detail": "..."} ----

    @Test
    void unreadableJsonIs400() {
        assertError(
                send(
                        HttpRequest.POST("/config", "{not json")
                                .contentType(MediaType.APPLICATION_JSON)),
                400);
        assertError(
                send(
                        HttpRequest.POST("/data/ingest", "not arrow".getBytes())
                                .contentType(MediaType.APPLICATION_OCTET_STREAM)),
                400);
    }

    @Test
    void unknownRouteAndMissingKeyAre404() {
        assertError(send(HttpRequest.GET("/no-such-route")), 404);
        Res missing = send(HttpRequest.GET("/cache/contract-missing"));
        assertError(missing, 404);
        assertThat(missing.body()).isEqualTo("{\"detail\":\"Key 'contract-missing' not found\"}");
    }

    @Test
    void wrongMethodIs405() {
        assertError(send(HttpRequest.DELETE("/config")), 405);
    }

    @Test
    void oversizedBatchAndOversizedBodyAre413() {
        byte[] bigBatch =
                ArrowBatches.encode(
                        LongStream.range(0, 2000).mapToObj(i -> row(i, "upsert")).toList());
        assertError(
                send(
                        HttpRequest.POST("/data/ingest", bigBatch)
                                .contentType(MediaType.APPLICATION_OCTET_STREAM)),
                413);
        assertError(
                send(
                        HttpRequest.POST("/data/ingest", new byte[2 * 1024 * 1024])
                                .contentType(MediaType.APPLICATION_OCTET_STREAM)),
                413);
    }

    @Test
    void wrongContentTypeIs415() {
        assertError(
                send(HttpRequest.POST("/data/ingest", "x").contentType(MediaType.TEXT_PLAIN)), 415);
    }

    @Test
    void validationAndConversionErrorsAre422() {
        assertError(
                send(
                        HttpRequest.POST("/cache", "{\"key\":\"\",\"value\":1}")
                                .contentType(MediaType.APPLICATION_JSON)),
                422);
        assertError(
                send(
                        HttpRequest.POST("/cache", "{\"key\":\"k\",\"value\":1,\"ttl_seconds\":0}")
                                .contentType(MediaType.APPLICATION_JSON)),
                422);
        assertError(send(HttpRequest.GET("/data?limit=0")), 422);
        assertError(send(HttpRequest.GET("/data?limit=abc")), 422);
        assertError(
                send(
                        HttpRequest.POST(
                                        "/data/ingest",
                                        ArrowBatches.encode(List.of(row(1, "insert"))))
                                .contentType(MediaType.APPLICATION_OCTET_STREAM)),
                422);
    }

    /** An unexpected store error is a 500 that never leaks the exception text. */
    @Test
    void unexpectedErrorIs500WithoutInternals() {
        jedis.set("contract-plain", "x");
        Res res =
                send(
                        HttpRequest.POST(
                                        "/cache",
                                        "{\"key\":\"contract-plain\",\"value\":{\"a\":1}}")
                                .contentType(MediaType.APPLICATION_JSON));
        assertError(res, 500);
        assertThat(res.body()).isEqualTo("{\"detail\":\"Internal Server Error\"}");
    }
}
