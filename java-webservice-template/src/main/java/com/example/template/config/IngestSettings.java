package com.example.template.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.util.List;

/**
 * Immutable ingest/transport configuration ({@code template.ingest.*}); defaults in
 * application.yml.
 *
 * @param transport {@code noop} (default: HTTP ingest only) or {@code solace}
 * @param maxBatchBytes largest accepted batch: the encoded payload of a Solace message, the decoded
 *     Arrow buffers of each HTTP batch
 * @param maxDisconnectSeconds the watchdog exits the process when the transport has been
 *     disconnected this long; 0 disables the watchdog
 * @param maxConsecutiveFailures the ingest loop exits the process after this many transport
 *     failures in a row without a successful bind; 0 disables it
 * @param lsmMaxRows retention cap: over it the oldest live rows are evicted and counted
 * @param lsmFlushRows memtable size that triggers a flush to a sorted run
 * @param lsmCompactionRuns run count that triggers a full compaction
 * @param lsmKeyColumns merge key; only {@code id} is supported
 * @param decoderMemoryBytes bound on direct memory held by all concurrent Arrow decodes
 * @param retryBaseDelaySeconds first transport/settlement backoff, doubled per failure
 * @param retryMaxDelaySeconds backoff ceiling
 */
@ConfigurationProperties("template.ingest")
public record IngestSettings(
        @NotNull @Pattern(regexp = "noop|solace") String transport,
        @Positive long maxBatchBytes,
        @PositiveOrZero long maxDisconnectSeconds,
        @PositiveOrZero int maxConsecutiveFailures,
        @PositiveOrZero double stalenessThresholdSeconds,
        @Min(1) int lsmMaxRows,
        @Min(1) int lsmFlushRows,
        @Min(2) int lsmCompactionRuns,
        @NotEmpty
                List<@Pattern(regexp = "id", message = "the Java store is keyed by id only") String>
                        lsmKeyColumns,
        @Positive long decoderMemoryBytes,
        @Positive double retryBaseDelaySeconds,
        @Positive double retryMaxDelaySeconds,
        @NotNull @Valid Solace solace) {

    /**
     * @param queue durable exclusive queue the consumer binds, created on start with the topic
     *     subscription added. One queue per replica: the LSM store is a per-instance cache.
     */
    @ConfigurationProperties("solace")
    public record Solace(
            @NotBlank String host,
            @Min(1) @Max(65535) int port,
            @NotBlank String vpn,
            @NotBlank String username,
            @NotNull String password,
            @NotBlank String topic,
            @NotBlank String queue) {}
}
