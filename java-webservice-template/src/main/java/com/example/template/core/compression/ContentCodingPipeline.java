package com.example.template.core.compression;

import com.example.template.config.CompressionSettings;
import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.context.event.BeanCreatedEventListener;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.netty.channel.ChannelPipelineCustomizer;
import io.micronaut.http.server.netty.NettyServerCustomizer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import jakarta.inject.Singleton;

/**
 * Installs the content-coding handlers right after the HTTP codec of every HTTP/1.1 connection (the
 * only protocol this service serves): the decoder first, then the compressor, so the compressor
 * records the normalised Accept-Encoding. Enabling HTTP/2 would need this revisited.
 */
@Singleton
public final class ContentCodingPipeline
        implements BeanCreatedEventListener<NettyServerCustomizer.Registry> {

    private static final String DECODER = "template-content-decoder";
    private static final String COMPRESSOR = "template-content-compressor";

    private final CompressionSettings settings;

    public ContentCodingPipeline(CompressionSettings settings) {
        this.settings = settings;
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
            pipeline.addAfter(
                    ChannelPipelineCustomizer.HANDLER_HTTP_SERVER_CODEC,
                    DECODER,
                    new ContentCodingHandler(settings.requireZstd()));
            if (settings.enabled()) {
                pipeline.addAfter(DECODER, COMPRESSOR, new ZstdContentCompressor(settings));
            }
        }
    }
}
