package org.iq80.leveldb.util;

import org.iq80.leveldb.impl.InternalKey;
import org.jboss.netty.buffer.ChannelBuffer;

public final class Buffers
{

    private Buffers()
    {
    }

    public static ChannelBuffer readLengthPrefixedBytes(ChannelBuffer buffer)
    {
        int length = VariableLengthQuantity.unpackInt(buffer);
        return buffer.readBytes(length);
    }

    public static void writeLengthPrefixedBytes(ChannelBuffer buffer, ChannelBuffer value)
    {
        VariableLengthQuantity.packInt(value.readableBytes(), buffer);
        buffer.writeBytes(value);
    }
}
