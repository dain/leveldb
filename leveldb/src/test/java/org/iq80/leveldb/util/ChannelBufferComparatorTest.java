package org.iq80.leveldb.util;

import com.google.common.base.Charsets;
import org.jboss.netty.buffer.ChannelBuffer;
import org.testng.annotations.Test;

import static org.iq80.leveldb.util.ChannelBufferComparator.CHANNEL_BUFFER_COMPARATOR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ChannelBufferComparatorTest
{
    @Test
    public void testChannelBufferComparison()
    {
        assertTrue(CHANNEL_BUFFER_COMPARATOR.compare(
                Buffers.copiedBuffer("beer/ipa", Charsets.UTF_8),
                Buffers.copiedBuffer("beer/ale", Charsets.UTF_8))
                > 0);

        assertTrue(CHANNEL_BUFFER_COMPARATOR.compare(
                Buffers.wrappedBuffer(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}),
                Buffers.wrappedBuffer(new byte[]{(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00}))
                > 0);

        assertTrue(CHANNEL_BUFFER_COMPARATOR.compare(
                Buffers.wrappedBuffer(new byte[]{(byte) 0xFF}),
                Buffers.wrappedBuffer(new byte[]{(byte) 0x00}))
                > 0);

        assertAllEqual(Buffers.copiedBuffer("abcdefghijklmnopqrstuvwxyz", Charsets.UTF_8),
                Buffers.copiedBuffer("abcdefghijklmnopqrstuvwxyz", Charsets.UTF_8));
    }

    public static void assertAllEqual(ChannelBuffer left, ChannelBuffer right)
    {
        for (int i = 0; i < left.readableBytes(); i++) {
            assertEquals(CHANNEL_BUFFER_COMPARATOR.compare(left.slice(0, i), right.slice(0, i)), 0);
            assertEquals(CHANNEL_BUFFER_COMPARATOR.compare(right.slice(0, i), left.slice(0, i)), 0);
        }
        // differ in last byte only
        for (int i = 1; i < left.readableBytes(); i++) {
            ChannelBuffer slice = right.copy(0, i);
            int lastReadableByte = slice.writerIndex() - 1;
            slice.setByte(lastReadableByte, slice.getByte(lastReadableByte) + 1);
            assertTrue(CHANNEL_BUFFER_COMPARATOR.compare(left.slice(0, i), slice) < 0);
            assertTrue(CHANNEL_BUFFER_COMPARATOR.compare(slice, left.slice(0, i)) > 0);
        }
    }

}
