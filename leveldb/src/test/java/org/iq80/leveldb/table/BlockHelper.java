package org.iq80.leveldb.table;

import com.google.common.base.Charsets;
import org.iq80.leveldb.SeekingIterator;
import org.jboss.netty.buffer.ChannelBuffer;
import org.iq80.leveldb.util.Buffers;
import org.testng.Assert;

import java.util.Arrays;
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

    public static <K, V> void assertSequence(SeekingIterator<K, V> seekingIterator, Entry<K, V>... entries)
    {
        assertSequence(seekingIterator, Arrays.asList(entries));
    }

    public static <K, V> void assertSequence(SeekingIterator<K, V> seekingIterator, Iterable<? extends Entry<K, V>> entries)
    {
        Assert.assertNotNull(seekingIterator, "blockIterator is not null");

        for (Entry<K, V> entry : entries) {
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

    public static <K, V> void assertEntryEquals(Entry<K, V> actual, Entry<K, V> expected)
    {
        if (actual.getKey() instanceof ChannelBuffer) {
            assertChannelBufferEquals((ChannelBuffer) actual.getKey(), (ChannelBuffer) expected.getKey());
            assertChannelBufferEquals((ChannelBuffer) actual.getValue(), (ChannelBuffer) expected.getValue());
        }
        assertEquals(actual, expected);
    }

    public static void assertChannelBufferEquals(ChannelBuffer actual, ChannelBuffer expected)
    {
        assertEquals(actual.toString(Charsets.UTF_8), expected.toString(Charsets.UTF_8));
    }

    public static String before(Entry<String, ?> expectedEntry)
    {
        String key = expectedEntry.getKey();
        int lastByte = key.charAt(key.length() - 1);
        return key.substring(0, key.length() - 1) + ((char) (lastByte - 1));
    }

    public static String after(Entry<String, ?> expectedEntry)
    {
        String key = expectedEntry.getKey();
        int lastByte = key.charAt(key.length() - 1);
        return key.substring(0, key.length() - 1) + ((char) (lastByte + 1));
    }

    public static ChannelBuffer before(Entry<ChannelBuffer, ?> expectedEntry)
    {
        ChannelBuffer channelBuffer = Buffers.copiedBuffer(expectedEntry.getKey());
        int lastByte = channelBuffer.readableBytes() - 1;
        channelBuffer.setByte(lastByte, channelBuffer.getByte(lastByte) - 1);
        return channelBuffer;
    }

    public static ChannelBuffer after(Entry<ChannelBuffer, ?> expectedEntry)
    {
        ChannelBuffer channelBuffer = Buffers.copiedBuffer(expectedEntry.getKey());
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
        return new BlockEntry(Buffers.copiedBuffer(key, UTF_8), Buffers.copiedBuffer(value, UTF_8));
    }
}
