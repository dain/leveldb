package org.iq80.leveldb.table;

import com.google.common.io.Closeables;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertTrue;

public class TableTest
{
    private File file;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEmptyFile()
            throws Exception
    {
        new Table(new Options(), fileChannel);
    }

    @Test
    public void testEmptyBlock()
            throws Exception
    {
        tableTest(Integer.MAX_VALUE, Integer.MAX_VALUE);
    }


    @Test
    public void testSingleEntrySingleBlock()
            throws Exception
    {
        tableTest(Integer.MAX_VALUE, Integer.MAX_VALUE,
                BlockHelper.createBlockEntry("name", "dain sundstrom"));
    }
    @Test
    public void testMultipleEntriesWithSingleBlock()
            throws Exception
    {
        List<BlockEntry> entries = Arrays.asList(
                BlockHelper.createBlockEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                BlockHelper.createBlockEntry("beer/ipa", "Lagunitas IPA"),
                BlockHelper.createBlockEntry("beer/stout", "Lagunitas Imperial Stout"),
                BlockHelper.createBlockEntry("scotch/light", "Oban 14"),
                BlockHelper.createBlockEntry("scotch/medium", "Highland Park"),
                BlockHelper.createBlockEntry("scotch/strong", "Lagavulin"));

        for (int i = 1; i < entries.size(); i++) {
            tableTest(Integer.MAX_VALUE, i, entries);
        }
    }

    @Test
    public void testMultipleEntriesWithMultipleBlock()
            throws Exception
    {
        List<BlockEntry> entries = Arrays.asList(
                BlockHelper.createBlockEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                BlockHelper.createBlockEntry("beer/ipa", "Lagunitas IPA"),
                BlockHelper.createBlockEntry("beer/stout", "Lagunitas Imperial Stout"),
                BlockHelper.createBlockEntry("scotch/light", "Oban 14"),
                BlockHelper.createBlockEntry("scotch/medium", "Highland Park"),
                BlockHelper.createBlockEntry("scotch/strong", "Lagavulin"));

        // one entry per block
        tableTest(1, Integer.MAX_VALUE, entries);

        // about 3 blocks
        tableTest(BlockHelper.estimateBlockSize(Integer.MAX_VALUE, entries) / 3, Integer.MAX_VALUE, entries);
    }

    private void tableTest(int blockSize, int blockRestartInterval, BlockEntry... entries)
            throws IOException
    {
        tableTest(blockSize, blockRestartInterval, asList(entries));
    }

    private void tableTest(int blockSize, int blockRestartInterval, List<BlockEntry> entries)
            throws IOException
    {
        Options options = new Options().setBlockSize(blockSize).setBlockRestartInterval(blockRestartInterval);
        TableBuilder builder = new TableBuilder(options, fileChannel);

        for (BlockEntry entry : entries) {
            builder.add(entry);
        }
        builder.finish();

        Table table = new Table(options, fileChannel);

        SeekingIterator seekingIterator = table.iterator();
        BlockHelper.assertSequence(seekingIterator, entries);

        seekingIterator.seekToFirst();
        BlockHelper.assertSequence(seekingIterator, entries);

        long lastApproximateOffset = 0;
        for (BlockEntry entry : entries) {
            List<BlockEntry> nextEntries = entries.subList(entries.indexOf(entry), entries.size());
            seekingIterator.seek(entry.getKey());
            BlockHelper.assertSequence(seekingIterator, nextEntries);

            seekingIterator.seek(BlockHelper.before(entry));
            BlockHelper.assertSequence(seekingIterator, nextEntries);

            seekingIterator.seek(BlockHelper.after(entry));
            BlockHelper.assertSequence(seekingIterator, nextEntries.subList(1, nextEntries.size()));

            long approximateOffset = table.getApproximateOffsetOf(entry.getKey());
            assertTrue(approximateOffset >= lastApproximateOffset);
            lastApproximateOffset = approximateOffset;
        }

        ChannelBuffer endKey = ChannelBuffers.wrappedBuffer(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
        seekingIterator.seek(endKey);
        BlockHelper.assertSequence(seekingIterator, Collections.<BlockEntry>emptyList());

        long approximateOffset = table.getApproximateOffsetOf(endKey);
        assertTrue(approximateOffset >= lastApproximateOffset);

    }

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        file = File.createTempFile("table", ".db");
        randomAccessFile = new RandomAccessFile(file, "rw");
        fileChannel = randomAccessFile.getChannel();
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        Closeables.closeQuietly(fileChannel);
        Closeables.closeQuietly(randomAccessFile);
        file.delete();
    }
}
