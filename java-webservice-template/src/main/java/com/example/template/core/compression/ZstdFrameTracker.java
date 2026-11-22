package com.example.template.core.compression;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Passes compressed bytes through unchanged while following the zstd frame layout (RFC 8878 section
 * 3.1), so the end of a body can be checked for a cut-off frame: Netty's ZstdDecoder streams
 * continuously and treats missing input as "more to come".
 */
final class ZstdFrameTracker extends ChannelInboundHandlerAdapter {

    private static final long MAGIC = 0xFD2FB528L;
    private static final long SKIPPABLE_MAGIC = 0x184D2A50L;
    private static final int[] DICT_ID_BYTES = {0, 1, 2, 4};

    private enum Field {
        MAGIC,
        DESCRIPTOR,
        BLOCK_HEADER,
        SKIPPABLE_SIZE,
        CORRUPT
    }

    private Field next = Field.MAGIC;
    private int want = 4;
    private int have;
    private long value;
    private long skip;
    private boolean checksum;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof ByteBuf buf) {
            scan(buf);
        }
        ctx.fireChannelRead(msg);
    }

    /** True between frames: every frame started so far has ended. */
    boolean atFrameBoundary() {
        return next == Field.MAGIC && have == 0 && skip == 0;
    }

    private void scan(ByteBuf buf) {
        int i = buf.readerIndex();
        int end = buf.writerIndex();
        while (i < end && next != Field.CORRUPT) {
            if (skip > 0) {
                int n = (int) Math.min(skip, end - i);
                skip -= n;
                i += n;
                continue;
            }
            value |= (long) (buf.getByte(i++) & 0xFF) << (8 * have);
            if (++have == want) {
                long field = value;
                value = 0;
                have = 0;
                onField(field);
            }
        }
    }

    private void onField(long field) {
        switch (next) {
            case MAGIC -> {
                if (field == MAGIC) {
                    expect(Field.DESCRIPTOR, 1);
                } else if ((field & ~0xFL) == SKIPPABLE_MAGIC) {
                    expect(Field.SKIPPABLE_SIZE, 4);
                } else {
                    next = Field.CORRUPT;
                }
            }
            case DESCRIPTOR -> {
                int sizeFlag = (int) (field >> 6);
                boolean singleSegment = (field & 0x20) != 0;
                checksum = (field & 0x04) != 0;
                int contentSizeBytes =
                        switch (sizeFlag) {
                            case 0 -> singleSegment ? 1 : 0;
                            case 1 -> 2;
                            case 2 -> 4;
                            default -> 8;
                        };
                skip =
                        (singleSegment ? 0 : 1)
                                + DICT_ID_BYTES[(int) (field & 3)]
                                + contentSizeBytes;
                expect(Field.BLOCK_HEADER, 3);
            }
            case BLOCK_HEADER -> {
                boolean last = (field & 1) != 0;
                int type = (int) ((field >> 1) & 3);
                if (type == 3) {
                    next = Field.CORRUPT; // reserved block type
                    return;
                }
                skip = type == 1 ? 1 : field >>> 3; // an RLE block carries one byte
                if (last) {
                    skip += checksum ? 4 : 0;
                    expect(Field.MAGIC, 4);
                } else {
                    expect(Field.BLOCK_HEADER, 3);
                }
            }
            case SKIPPABLE_SIZE -> {
                skip = field;
                expect(Field.MAGIC, 4);
            }
            case CORRUPT -> {
                // unreachable: scanning stops
            }
        }
    }

    private void expect(Field field, int bytes) {
        next = field;
        want = bytes;
    }
}
