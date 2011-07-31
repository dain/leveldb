package org.iq80.leveldb.table;

import com.google.common.base.Charsets;
import org.iq80.leveldb.SeekingIterator;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.testng.Assert;

import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import static com.google.common.base.Charsets.UTF_8;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_BYTE;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class BlockHelper
{
    public static int estimateBlockSize(int blockRestartInterval, List<BlockEntry> entries)
    {
        if (entries.isEmpty()) {
            return SIZE_OF_INT;
        }
        int restartCount = (int) Math.ceil(1.0 * entries.size() / blockRestartInterval);
        return estimateEntriesSize(blockRestartInterval, entries) +
                (restartCount * SIZE_OF_INT) +
                SIZE_OF_INT;
    }

    public static void assertSequence(SeekingIterator<ChannelBuffer, ChannelBuffer> seekingIterator, Iterable<? extends Entry<ChannelBuffer, ChannelBuffer>> entries)
    {
        Assert.assertNotNull(seekingIterator, "blockIterator is not null");

        for (Entry<ChannelBuffer, ChannelBuffer> entry : entries) {
            assertTrue(seekingIterator.hasNext());
            assertEntryEquals(seekingIterator.peek(), entry);
            assertEntryEquals(seekingIterator.next(), entry);
        }
        assertFalse(seekingIterator.hasNext());

        try {
            seekingIterator.peek();
            fail("expected NoSuchElementException");
        }
        catch (NoSuchElementException expected) {
        }
        try {
            seekingIterator.next();
            fail("expected NoSuchElementException");
        }
        catch (NoSuchElementException expected) {
        }
    }

    public static void assertEntryEquals(Entry<ChannelBuffer, ChannelBuffer> actual, Entry<ChannelBuffer, ChannelBuffer> expected)
    {
        assertEquals(actual.getKey().toString(Charsets.UTF_8), expected.getKey().toString(Charsets.UTF_8));
        assertEquals(actual.getValue().toString(Charsets.UTF_8), expected.getValue().toString(Charsets.UTF_8));
        assertEquals(actual, expected);
    }

    public static ChannelBuffer before(Entry<ChannelBuffer, ?> expectedEntry)
    {
        ChannelBuffer channelBuffer = ChannelBuffers.copiedBuffer(expectedEntry.getKey());
        int lastByte = channelBuffer.readableBytes() - 1;
        channelBuffer.setByte(lastByte, channelBuffer.getByte(lastByte) - 1);
        return channelBuffer;
    }

    public static ChannelBuffer after(Entry<ChannelBuffer, ?> expectedEntry)
    {
        ChannelBuffer channelBuffer = ChannelBuffers.copiedBuffer(expectedEntry.getKey());
        int lastByte = channelBuffer.readableBytes() - 1;
        channelBuffer.setByte(lastByte, channelBuffer.getByte(lastByte) + 1);
        return channelBuffer;
    }

    public static int estimateEntriesSize(int blockRestartInterval, List<BlockEntry> entries)
    {
        int size = 0;
        ChannelBuffer previousKey = null;
        int restartBlockCount = 0;
        for (BlockEntry entry : entries) {
            int nonSharedBytes;
            if (restartBlockCount < blockRestartInterval) {
                nonSharedBytes = entry.getKey().readableBytes() - BlockBuilder.calculateSharedBytes(entry.getKey(), previousKey);
            }
            else {
                nonSharedBytes = entry.getKey().readableBytes();
                restartBlockCount = 0;
            }
            size += nonSharedBytes +
                    entry.getValue().readableBytes() +
                    (SIZE_OF_BYTE * 3); // 3 bytes for sizes

            previousKey = entry.getKey();
            restartBlockCount++;

        }
        return size;
    }

    static BlockEntry createBlockEntry(String key, String value)
    {
        return new BlockEntry(ChannelBuffers.copiedBuffer(key, UTF_8), ChannelBuffers.copiedBuffer(value, UTF_8));
    }
}
