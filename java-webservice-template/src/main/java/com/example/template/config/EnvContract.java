package com.example.template.config;

import io.micronaut.context.ApplicationContextBuilder;
import io.micronaut.context.ApplicationContextConfigurer;
import io.micronaut.context.annotation.ContextConfigurer;
import io.micronaut.context.env.PropertySource;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Shared env vars whose shape differs from Micronaut's properties: URL-style Postgres and host/port
 * ClickHouse settings become JDBC URLs. Plain names are mapped in application.yml.
 */
@ContextConfigurer
public final class EnvContract implements ApplicationContextConfigurer {

    @Override
    public void configure(ApplicationContextBuilder builder) {
        Map<String, Object> props = translate(System.getenv());
        if (!props.isEmpty()) {
            builder.propertySources(PropertySource.of("env-contract", props));
        }
    }

    static Map<String, Object> translate(Map<String, String> env) {
        Map<String, Object> props = new HashMap<>();
        // Not in application.yml: a default micronaut.server.host would also become the Host header
        // of the test client, and Micronaut already binds every interface when it is unset.
        if (env.containsKey("SERVER_HOST")) {
            props.put("micronaut.server.host", env.get("SERVER_HOST"));
        }
        String pg = env.get("POSTGRES_URL");
        if (pg != null) {
            URI uri = URI.create(pg);
            props.put(
                    "datasources.default.url",
                    "jdbc:postgresql://" + uri.getHost() + ":" + port(uri, 5432) + uri.getPath());
            String userInfo = uri.getRawUserInfo();
            if (userInfo != null) {
                String[] parts = userInfo.split(":", 2);
                props.put("datasources.default.username", decode(parts[0]));
                if (parts.length == 2) {
                    props.put("datasources.default.password", decode(parts[1]));
                }
            }
        }
        if (env.containsKey("CLICKHOUSE_HOST")) {
            props.put(
                    "datasources.clickhouse.url",
                    "jdbc:ch://%s:%s/%s"
                            .formatted(
                                    env.get("CLICKHOUSE_HOST"),
                                    env.getOrDefault("CLICKHOUSE_PORT", "8123"),
                                    env.getOrDefault("CLICKHOUSE_DATABASE", "default")));
        }
        String acquire = env.get("POSTGRES_ACQUIRE_TIMEOUT_SECONDS");
        if (acquire != null) {
            props.put(
                    "datasources.default.connection-timeout",
                    String.valueOf(Math.round(Double.parseDouble(acquire) * 1000)));
        }
        return props;
    }

    private static int port(URI uri, int fallback) {
        return uri.getPort() == -1 ? fallback : uri.getPort();
    }

    private static String decode(String s) {
        return URLDecoder.decode(s, StandardCharsets.UTF_8);
    }
}
