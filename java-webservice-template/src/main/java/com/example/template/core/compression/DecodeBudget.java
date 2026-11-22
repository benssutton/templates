package com.example.template.core.compression;

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.TooLongFrameException;

/**
 * Bounds one request body's decoding at max-request-size. As the last handler of the decoder
 * channel it forwards decoded bytes while the running total is within the limit and releases the
 * rest. As that channel's allocator it refuses new buffers once past the limit: Netty's decoders
 * expand all the input they are handed in one call (removing them only takes effect afterwards),
 * and a 64 KiB read of RLE-heavy zstd can mean gigabytes of work, so failing their next output
 * buffer is what stops them mid-input. The refusal covers allocations made through the decoder
 * channel's allocator (ctx.alloc()), which is where both decoders get their output buffers.
 */
final class DecodeBudget extends ChannelInboundHandlerAdapter {

    private final long max;
    private final ByteBufAllocator allocator;
    private long decoded;
    private boolean exceeded;

    DecodeBudget(long max, ByteBufAllocator parent) {
        this.max = max;
        this.allocator = new Allocator(parent);
    }

    ByteBufAllocator allocator() {
        return allocator;
    }

    /** Every decoded byte that reached this handler, forwarded or not. */
    long decodedBytes() {
        return decoded;
    }

    boolean exceeded() {
        return exceeded;
    }

    TooLongFrameException failure() {
        return new TooLongFrameException("Decompressed request body exceeds " + max + " bytes");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf buf = (ByteBuf) msg;
        decoded += buf.readableBytes();
        exceeded |= decoded > max;
        if (exceeded) {
            buf.release();
        } else {
            ctx.fireChannelRead(buf);
        }
    }

    private final class Allocator extends AbstractByteBufAllocator {
        private final ByteBufAllocator parent;

        Allocator(ByteBufAllocator parent) {
            this.parent = parent;
        }

        @Override
        protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
            stopIfExceeded();
            return parent.heapBuffer(initialCapacity, maxCapacity);
        }

        @Override
        protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
            stopIfExceeded();
            return parent.directBuffer(initialCapacity, maxCapacity);
        }

        @Override
        public boolean isDirectBufferPooled() {
            return parent.isDirectBufferPooled();
        }

        private void stopIfExceeded() {
            if (exceeded) {
                throw failure();
            }
        }
    }
}
