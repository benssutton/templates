package com.example.template.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class McpHostFilterTest {

    private static McpHostFilter filter(String... allowedHosts) {
        return new McpHostFilter(List.of(allowedHosts));
    }

    @Test
    void entryWithoutPortMatchesAnyPort() {
        McpHostFilter f = filter("localhost", "app");
        assertThat(f.isAllowedHost("localhost")).isTrue();
        assertThat(f.isAllowedHost("localhost:8443")).isTrue();
        assertThat(f.isAllowedHost("APP:443")).isTrue();
        assertThat(f.isAllowedHost("localhost.evil.example")).isFalse();
        assertThat(f.isAllowedHost("evil.example")).isFalse();
    }

    @Test
    void entryWithPortMatchesOnlyThatPort() {
        McpHostFilter f = filter("api.example.com:8443");
        assertThat(f.isAllowedHost("api.example.com:8443")).isTrue();
        assertThat(f.isAllowedHost("api.example.com")).isFalse();
        assertThat(f.isAllowedHost("api.example.com:9000")).isFalse();
    }

    /** A prefix test on the raw authority let these through: compare the parsed host. */
    @Test
    void userinfoAndPrefixTricksAreRejected() {
        McpHostFilter f = filter("localhost");
        assertThat(f.isAllowedOrigin("https://localhost:1@evil.example")).isFalse();
        assertThat(f.isAllowedOrigin("https://localhost@evil.example")).isFalse();
        assertThat(f.isAllowedHost("localhost:1@evil.example")).isFalse();
        assertThat(f.isAllowedHost("localhost:notaport")).isFalse();
        assertThat(f.isAllowedOrigin("not a uri")).isFalse();
        assertThat(f.isAllowedOrigin("https://localhost:8443")).isTrue();
    }

    @Test
    void ipv6EntriesParse() {
        McpHostFilter f = filter("[::1]");
        assertThat(f.isAllowedHost("[::1]:8443")).isTrue();
        assertThat(f.isAllowedHost("[::2]:8443")).isFalse();
    }

    @Test
    void badEntriesFailAtConstruction() {
        for (String bad : new String[] {"evil.example/path", "user@host", "host:99999", ""}) {
            assertThatThrownBy(() -> filter(bad))
                    .as(bad)
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> new McpHostFilter(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
