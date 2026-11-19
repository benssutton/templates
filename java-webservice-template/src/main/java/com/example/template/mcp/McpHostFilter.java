package com.example.template.mcp;

import com.example.template.config.AppSettings;
import com.example.template.dto.ErrorDetail;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.RequestFilter;
import io.micronaut.http.annotation.ServerFilter;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * DNS-rebinding protection for {@code /mcp}, which the MCP spec requires of Streamable HTTP servers
 * and micronaut-mcp does not provide. The Host header (and Origin, when present) must name a host
 * in {@code template.mcp-allowed-hosts}; otherwise 403 {@code {"detail"}}. An entry without a port
 * matches any port.
 *
 * <p>Entries are parsed once, here, into (host, port) and a bad entry fails startup. Requests are
 * compared on their parsed host and port, never on a raw string prefix, so {@code
 * https://localhost:1@evil.example} is {@code evil.example}, not localhost.
 */
@ServerFilter({"/mcp", "/mcp/**"})
public class McpHostFilter {

    private static final int ANY_PORT = -1;

    private record Allowed(String host, int port) {}

    private record Authority(String host, int port) {}

    private final List<Allowed> allowed;

    @Inject
    public McpHostFilter(AppSettings settings) {
        this(settings.mcpAllowedHosts());
    }

    /** {@code allowedHosts}: a JSON array of strings or a comma-separated list. */
    public McpHostFilter(String allowedHosts) {
        List<Allowed> parsed = new ArrayList<>();
        for (String entry : splitEntries(allowedHosts)) {
            Authority a = parseAuthority(entry);
            if (a == null) {
                throw new IllegalArgumentException(
                        "template.mcp-allowed-hosts: not a host[:port]: '" + entry + "'");
            }
            parsed.add(new Allowed(a.host(), a.port()));
        }
        if (parsed.isEmpty()) {
            throw new IllegalArgumentException(
                    "template.mcp-allowed-hosts must name at least one host "
                            + "(an empty list would disable the /mcp DNS-rebinding protection)");
        }
        this.allowed = List.copyOf(parsed);
    }

    private static List<String> splitEntries(String raw) {
        String s = raw == null ? "" : raw.strip();
        List<String> out = new ArrayList<>();
        // A JSON array starts with [" or is []; a bare [ starts an IPv6 literal.
        String afterBracket = s.startsWith("[") ? s.substring(1).strip() : "";
        if (afterBracket.startsWith("\"") || afterBracket.equals("]")) {
            if (!s.endsWith("]")) {
                throw new IllegalArgumentException(
                        "template.mcp-allowed-hosts: unterminated JSON array");
            }
            String body = s.substring(1, s.length() - 1).strip();
            if (body.isEmpty()) {
                return out;
            }
            for (String item : body.split(",")) {
                String t = item.strip();
                if (t.length() < 2 || !t.startsWith("\"") || !t.endsWith("\"")) {
                    throw new IllegalArgumentException(
                            "template.mcp-allowed-hosts: JSON array must hold strings, got " + t);
                }
                out.add(t.substring(1, t.length() - 1).strip());
            }
            return out;
        }
        for (String item : s.split(",")) {
            if (!item.isBlank()) {
                out.add(item.strip());
            }
        }
        return out;
    }

    /** {@code host[:port]} only: userinfo, path, query or a bad port make it null. */
    @Nullable
    private static Authority parseAuthority(String authority) {
        try {
            return fromUri(new URI("//" + authority));
        } catch (URISyntaxException e) {
            return null;
        }
    }

    /** A null host means the authority did not parse as host[:port] (e.g. a non-numeric port). */
    @Nullable
    private static Authority fromUri(URI uri) {
        if (uri.getHost() == null
                || uri.getRawUserInfo() != null
                || uri.getPort() > 65535
                || !uri.getRawPath().isEmpty()
                || uri.getRawQuery() != null
                || uri.getRawFragment() != null) {
            return null;
        }
        return new Authority(uri.getHost().toLowerCase(Locale.ROOT), uri.getPort());
    }

    @RequestFilter
    @Nullable
    public HttpResponse<?> checkHostAndOrigin(HttpRequest<?> request) {
        String host = request.getHeaders().get(HttpHeaders.HOST);
        if (host == null || !isAllowedHost(host)) {
            return forbidden("Invalid Host header");
        }
        String origin = request.getHeaders().get(HttpHeaders.ORIGIN);
        if (origin != null && !isAllowedOrigin(origin)) {
            return forbidden("Invalid Origin header");
        }
        return null; // continue the chain
    }

    private static HttpResponse<ErrorDetail> forbidden(String detail) {
        return HttpResponse.<ErrorDetail>status(HttpStatus.FORBIDDEN)
                .contentType(MediaType.APPLICATION_JSON_TYPE)
                .body(new ErrorDetail(detail));
    }

    boolean isAllowedHost(String hostHeader) {
        Authority a = parseAuthority(hostHeader.strip());
        return a != null && matches(a);
    }

    boolean isAllowedOrigin(String origin) {
        try {
            URI uri = new URI(origin.strip());
            if (uri.getScheme() == null) {
                return false;
            }
            Authority a = fromUri(uri);
            if (a == null) {
                return false;
            }
            int port = a.port() != ANY_PORT ? a.port() : defaultPort(uri.getScheme());
            return matches(new Authority(a.host(), port));
        } catch (URISyntaxException e) {
            return false;
        }
    }

    private static int defaultPort(String scheme) {
        return switch (scheme.toLowerCase(Locale.ROOT)) {
            case "https" -> 443;
            case "http" -> 80;
            default -> ANY_PORT;
        };
    }

    private boolean matches(Authority a) {
        for (Allowed entry : allowed) {
            if (entry.host().equals(a.host())
                    && (entry.port() == ANY_PORT || entry.port() == a.port())) {
                return true;
            }
        }
        return false;
    }
}
