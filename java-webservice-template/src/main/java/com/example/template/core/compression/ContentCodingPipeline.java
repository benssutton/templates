package com.example.template.core.compression;

import com.example.template.config.CompressionSettings;
import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.context.event.BeanCreatedEventListener;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.netty.channel.ChannelPipelineCustomizer;
import io.micronaut.http.server.HttpServerConfiguration;
import io.micronaut.http.server.netty.NettyServerCustomizer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.compression.StandardCompressionOptions;
import io.netty.handler.codec.compression.Zstd;
import io.netty.handler.codec.compression.ZstdOptions;
import jakarta.inject.Singleton;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Installs the content-coding handlers right after the HTTP codec of every HTTP/1.1 connection (the
 * only protocol this service serves): the decoder first, then the compressor, so the compressor
 * records the normalised Accept-Encoding. Enabling HTTP/2 would need this revisited.
 */
@Singleton
public final class ContentCodingPipeline
        implements BeanCreatedEventListener<NettyServerCustomizer.Registry> {

    private static final Logger LOG = LoggerFactory.getLogger(ContentCodingPipeline.class);
    private static final String DECODER = "template-content-decoder";
    private static final String COMPRESSOR = "template-content-compressor";

    private final CompressionSettings settings;
    private final long maxRequestSize;
    private final ZstdOptions zstdOptions;
    private final AtomicBoolean codecMissingLogged = new AtomicBoolean();

    public ContentCodingPipeline(CompressionSettings settings, HttpServerConfiguration server) {
        // Requests are decoded even with compression off, so zstd is needed either way; without
        // this check every zstd request would fail instead of the service failing to start.
        if (!Zstd.isAvailable()) {
            throw new IllegalStateException(
                    "zstd native library (zstd-jni) is unavailable on this platform", Zstd.cause());
        }
        this.settings = settings;
        this.maxRequestSize = server.getMaxRequestSize();
        ZstdOptions defaults = StandardCompressionOptions.zstd();
        this.zstdOptions =
                StandardCompressionOptions.zstd(
                        settings.zstdLevel(), defaults.blockSize(), defaults.maxEncodeSize());
    }

    @Override
    public NettyServerCustomizer.Registry onCreated(
            BeanCreatedEvent<NettyServerCustomizer.Registry> event) {
        NettyServerCustomizer.Registry registry = event.getBean();
        registry.register(new Customizer(null));
        return registry;
    }

    private final class Customizer implements NettyServerCustomizer {
        @Nullable private final Channel channel;

        Customizer(@Nullable Channel channel) {
            this.channel = channel;
        }

        @Override
        public NettyServerCustomizer specializeForChannel(Channel channel, ChannelRole role) {
            return new Customizer(channel);
        }

        @Override
        public void onStreamPipelineBuilt() {
            if (channel == null) {
                return;
            }
            ChannelPipeline pipeline = channel.pipeline();
            // Without the HTTP/1.1 codec (e.g. HTTP/2 switched on) there is nowhere to install
            // the handlers; throwing would fail every connection, so report it once instead.
            if (pipeline.get(ChannelPipelineCustomizer.HANDLER_HTTP_SERVER_CODEC) == null) {
                if (codecMissingLogged.compareAndSet(false, true)) {
                    LOG.error(
                            "No {} handler in the pipeline: request decoding and response"
                                    + " compression are off for such connections",
                            ChannelPipelineCustomizer.HANDLER_HTTP_SERVER_CODEC);
                }
                return;
            }
            pipeline.addAfter(
                    ChannelPipelineCustomizer.HANDLER_HTTP_SERVER_CODEC,
                    DECODER,
                    new ContentCodingHandler(settings.requireZstd(), maxRequestSize));
            if (settings.enabled()) {
                pipeline.addAfter(
                        DECODER,
                        COMPRESSOR,
                        new ZstdContentCompressor(settings.effectiveMinBytes(), zstdOptions));
            }
        }
    }
}
