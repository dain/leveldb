package org.iq80.leveldb.util;

import org.jboss.netty.buffer.ChannelBuffers;
import org.testng.annotations.Test;

import static org.iq80.leveldb.util.ChannelBufferComparator.CHANNEL_BUFFER_COMPARATOR;
import static org.testng.Assert.assertTrue;

public class ChannelBufferComparatorTest
{
    @Test
    public void testChannelBufferComparison()
    {
        assertTrue(CHANNEL_BUFFER_COMPARATOR.compare(
                ChannelBuffers.wrappedBuffer(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}),
                ChannelBuffers.wrappedBuffer(new byte[]{(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00}))
                > 0);

        assertTrue(CHANNEL_BUFFER_COMPARATOR.compare(
                ChannelBuffers.wrappedBuffer(new byte[]{(byte) 0xFF}),
                ChannelBuffers.wrappedBuffer(new byte[]{(byte) 0x00}))
                > 0);
    }

}
