package org.iq80.leveldb.table;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.testng.Assert;

import java.util.List;
import java.util.NoSuchElementException;

import static com.google.common.base.Charsets.UTF_8;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_BYTE;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class BlockHelper
{
    static int estimateBlockSize(int blockRestartInterval, List<BlockEntry> entries)
    {
        if (entries.isEmpty()) {
            return SIZE_OF_INT;
        }
        int restartCount = (int) Math.ceil(1.0 * entries.size() / blockRestartInterval);
        return estimateEntriesSize(blockRestartInterval, entries) +
                (restartCount * SIZE_OF_INT) +
                SIZE_OF_INT;
    }

    static void assertSequence(SeekingIterator seekingIterator, Iterable<BlockEntry> entries)
    {
        Assert.assertNotNull(seekingIterator, "blockIterator is not null");

        for (BlockEntry entry : entries) {
            assertTrue(seekingIterator.hasNext());
            assertEquals(seekingIterator.peek(), entry);
            assertEquals(seekingIterator.next(), entry);
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

    static ChannelBuffer before(BlockEntry expectedEntry)
    {
        ChannelBuffer channelBuffer = ChannelBuffers.copiedBuffer(expectedEntry.getKey());
        int lastByte = channelBuffer.readableBytes() - 1;
        channelBuffer.setByte(lastByte, channelBuffer.getByte(lastByte) - 1);
        return channelBuffer;
    }

    static ChannelBuffer after(BlockEntry expectedEntry)
    {
        ChannelBuffer channelBuffer = ChannelBuffers.copiedBuffer(expectedEntry.getKey());
        int lastByte = channelBuffer.readableBytes() - 1;
        channelBuffer.setByte(lastByte, channelBuffer.getByte(lastByte) + 1);
        return channelBuffer;
    }

    static int estimateEntriesSize(int blockRestartInterval, List<BlockEntry> entries)
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
        return new BlockEntry(toChannelBuffer(key), toChannelBuffer(value));
    }

    static ChannelBuffer toChannelBuffer(String value)
    {
        return ChannelBuffers.wrappedBuffer(value.getBytes(UTF_8));
    }
}
