package com.example.template.config;

import io.micronaut.context.ApplicationContextBuilder;
import io.micronaut.context.ApplicationContextConfigurer;
import io.micronaut.context.annotation.ContextConfigurer;
import io.micronaut.context.env.PropertySource;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

    /** Blank values count as absent, so an empty assignment in a .env file means "unset". */
    static Map<String, Object> translate(Map<String, String> env) {
        Map<String, Object> props = new HashMap<>();
        // Not in application.yml: a default micronaut.server.host would also become the Host header
        // of the test client, and Micronaut already binds every interface when it is unset.
        String host = value(env, "SERVER_HOST");
        if (host != null) {
            props.put("micronaut.server.host", host);
        }
        // Set only when non-blank: an empty issuer would make Micronaut reject every token.
        String issuer = value(env, "AUTH_JWT_ISSUER");
        if (issuer != null) {
            props.put("micronaut.security.token.jwt.claims-validators.issuer", issuer);
        }
        String audience = value(env, "AUTH_JWT_AUDIENCE");
        if (audience != null) {
            props.put("micronaut.security.token.jwt.claims-validators.audience", audience);
        }
        String hosts = value(env, "MCP_ALLOWED_HOSTS");
        if (hosts != null && isJsonArray(hosts)) {
            props.put("template.mcp-allowed-hosts", String.join(",", jsonStrings(hosts)));
        }
        String pg = value(env, "POSTGRES_URL");
        if (pg != null) {
            URI uri = URI.create(pg);
            String query = uri.getRawQuery();
            props.put(
                    "datasources.default.url",
                    "jdbc:postgresql://"
                            + uri.getHost()
                            + ":"
                            + port(uri, 5432)
                            + uri.getRawPath()
                            + (query == null ? "" : "?" + query));
            String userInfo = uri.getRawUserInfo();
            if (userInfo != null) {
                String[] parts = userInfo.split(":", 2);
                props.put("datasources.default.username", decode(parts[0]));
                if (parts.length == 2) {
                    props.put("datasources.default.password", decode(parts[1]));
                }
            }
        }
        String chHost = value(env, "CLICKHOUSE_HOST");
        if (chHost != null) {
            props.put(
                    "datasources.clickhouse.url",
                    "jdbc:ch://%s:%s/%s"
                            .formatted(
                                    chHost,
                                    valueOr(env, "CLICKHOUSE_PORT", "8123"),
                                    valueOr(env, "CLICKHOUSE_DATABASE", "default")));
        }
        String acquire = value(env, "POSTGRES_ACQUIRE_TIMEOUT_SECONDS");
        if (acquire != null) {
            props.put("datasources.default.connection-timeout", String.valueOf(millis(acquire)));
        }
        return props;
    }

    /** A JSON array starts with [" or is []; a bare [ starts an IPv6 literal. */
    private static boolean isJsonArray(String s) {
        if (!s.startsWith("[")) {
            return false;
        }
        String after = s.substring(1).strip();
        return after.startsWith("\"") || after.equals("]");
    }

    private static List<String> jsonStrings(String s) {
        if (!s.endsWith("]")) {
            throw new IllegalArgumentException("MCP_ALLOWED_HOSTS: unterminated JSON array");
        }
        String body = s.substring(1, s.length() - 1).strip();
        List<String> out = new ArrayList<>();
        if (body.isEmpty()) {
            return out;
        }
        for (String item : body.split(",")) {
            String t = item.strip();
            if (t.length() < 2 || !t.startsWith("\"") || !t.endsWith("\"")) {
                throw new IllegalArgumentException(
                        "MCP_ALLOWED_HOSTS: JSON array must hold strings, got " + t);
            }
            out.add(t.substring(1, t.length() - 1).strip());
        }
        return out;
    }

    private static long millis(String seconds) {
        try {
            return Math.round(Double.parseDouble(seconds) * 1000);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "POSTGRES_ACQUIRE_TIMEOUT_SECONDS must be a number of seconds, got '"
                            + seconds
                            + "'",
                    e);
        }
    }

    private static String value(Map<String, String> env, String name) {
        String v = env.get(name);
        return v == null || v.isBlank() ? null : v.strip();
    }

    private static String valueOr(Map<String, String> env, String name, String fallback) {
        String v = value(env, name);
        return v == null ? fallback : v;
    }

    private static int port(URI uri, int fallback) {
        return uri.getPort() == -1 ? fallback : uri.getPort();
    }

    private static String decode(String s) {
        return URLDecoder.decode(s, StandardCharsets.UTF_8);
    }
}
