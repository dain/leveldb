/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.leveldb.table;

import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.SeekingIterator;
import org.iq80.leveldb.util.Closeables;
import org.iq80.leveldb.util.LRUCache;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.Slices;
import org.iq80.leveldb.util.Snappy;
import org.iq80.leveldb.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class TableTest
{
    private File file;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;

    protected abstract Table createTable(String name, FileChannel fileChannel, Comparator<Slice> comparator, boolean verifyChecksums, FilterPolicy filterPolicy)
            throws IOException;

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEmptyFile()
            throws Exception
    {
        createTable(file.getAbsolutePath(), fileChannel, new BytewiseComparator(), true, null);
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
        List<BlockEntry> entries = asList(
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
        List<BlockEntry> entries = asList(
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

    private static final class KVMap extends ConcurrentSkipListMap<Slice, Slice>
    {
        public KVMap(UserComparator useComparator)
        {
            super(new STLLessThan(useComparator));
        }

        void add(String key, Slice value)
        {
            put(TestUtils.asciiToSlice(key), value);
        }
    }

    private static class STLLessThan implements Comparator<Slice>
    {
        private UserComparator useComparator;

        public STLLessThan(UserComparator useComparator)
        {
            this.useComparator = useComparator;
        }

        @Override
        public int compare(Slice o1, Slice o2)
        {
            return useComparator.compare(o1, o2);
        }
    }

    @Test
    public void testTableTestApproximateOffsetOfPlain() throws Exception
    {
        TableConstructor c = new TableConstructor(new BytewiseComparator());
        c.add("k01", "hello");
        c.add("k02", "hello2");
        c.add("k03", TestUtils.longString(10000, 'x'));
        c.add("k04", TestUtils.longString(200000, 'x'));
        c.add("k05", TestUtils.longString(300000, 'x'));
        c.add("k06", "hello3");
        c.add("k07", TestUtils.longString(100000, 'x'));

        final Options options = new Options();
        options.blockSize(1024);
        options.compressionType(CompressionType.NONE);
        c.finish(options);

        assertBetween(c.approximateOffsetOf("abc"), 0, 0);
        assertBetween(c.approximateOffsetOf("k01"), 0, 0);
        assertBetween(c.approximateOffsetOf("k01a"), 0, 0);
        assertBetween(c.approximateOffsetOf("k02"), 0, 0);
        assertBetween(c.approximateOffsetOf("k03"), 0, 0);
        assertBetween(c.approximateOffsetOf("k04"), 10000, 11000);
        assertBetween(c.approximateOffsetOf("k04a"), 210000, 211000);
        assertBetween(c.approximateOffsetOf("k05"), 210000, 211000);
        assertBetween(c.approximateOffsetOf("k06"), 510000, 511000);
        assertBetween(c.approximateOffsetOf("k07"), 510000, 511000);
        assertBetween(c.approximateOffsetOf("xyz"), 610000, 612000);
    }

    @Test
    public void testTableTestApproximateOffsetOfCompressed() throws Exception
    {
        if (!Snappy.available()) {
            System.out.println("skipping compression tests");
            return;
        }

        Random rnd = new Random(301);
        TableConstructor c = new TableConstructor(new BytewiseComparator());
        c.add("k01", "hello");
        c.add("k02", TestUtils.compressibleString(rnd, 0.25, 10000));
        c.add("k03", "hello3");
        c.add("k04", TestUtils.compressibleString(rnd, 0.25, 10000));

        Options options = new Options();
        options.blockSize(1024);
        options.compressionType(CompressionType.SNAPPY);
        c.finish(options);

        // Expected upper and lower bounds of space used by compressible strings.
        int kSlop = 1000;  // Compressor effectiveness varies.
        int expected = 2500;  // 10000 * compression ratio (0.25)
        int minZ = expected - kSlop;
        int maxZ = expected + kSlop;

        assertBetween(c.approximateOffsetOf("abc"), 0, kSlop);
        assertBetween(c.approximateOffsetOf("k01"), 0, kSlop);
        assertBetween(c.approximateOffsetOf("k02"), 0, kSlop);
        // Have now emitted a large compressible string, so adjust expected offset.
        assertBetween(c.approximateOffsetOf("k03"), minZ, maxZ);
        assertBetween(c.approximateOffsetOf("k04"), minZ, maxZ);
        // Have now emitted two large compressible strings, so adjust expected offset.
        assertBetween(c.approximateOffsetOf("xyz"), 2 * minZ, 2 * maxZ);
    }

    static void assertBetween(long val, long low, long high)
    {
        assertTrue((val >= low) && (val <= high),
                String.format("Value %s is not in range [%s, %s]", val, low, high));
    }

    private abstract static class Constructor implements AutoCloseable
    {
        private final KVMap kvMap;
        private final UserComparator comparator;

        public Constructor(final UserComparator comparator)
        {
            this.comparator = comparator;
            this.kvMap = new KVMap(this.comparator);

        }

        void add(Slice key, Slice value)
        {
            kvMap.put(key, value);
        }

        void add(String key, Slice value)
        {
            kvMap.put(TestUtils.asciiToSlice(key), value);
        }

        void add(String key, String value)
        {
            add(key, TestUtils.asciiToSlice(value));
        }

        public final KVMap finish(Options options) throws IOException
        {
            finish(options, comparator, kvMap);
            return kvMap;

        }

        @Override
        public void close() throws Exception
        {
        }

        protected abstract void finish(Options options, UserComparator comparator, KVMap kvMap) throws IOException;

        public abstract SeekingIterator<Slice, Slice> iterator();
    }

    public static class TableConstructor extends Constructor
    {
        private Table table;

        public TableConstructor(UserComparator comparator)
        {
            super(comparator);
        }

        @Override
        protected void finish(Options options, UserComparator comp, KVMap data) throws IOException
        {
            StringSink sink = new StringSink();
            TableBuilder builder = new TableBuilder(options, sink, comp);

            for (Map.Entry<Slice, Slice> e : data.entrySet()) {
                builder.add(e.getKey(), e.getValue());
            }
            builder.finish();
            sink.close();

            assertEquals(sink.content.length, builder.getFileSize());

            // Open the table
            StringSource source = new StringSource(sink.content);
            LRUCache<BlockHandle, Slice> blockCache = new LRUCache<>(options.cacheSize() > 0 ? (int) options.cacheSize() : 8 << 20, new BlockHandleSliceWeigher());
            table = new Table(source, comp, options.verifyChecksums(), blockCache, (FilterPolicy) options.filterPolicy());
        }

        public long approximateOffsetOf(String key)
        {
            return table.getApproximateOffsetOf(TestUtils.asciiToSlice(key));
        }

        @Override
        public SeekingIterator<Slice, Slice> iterator()
        {
            return table.iterator();
        }
    }

    private static class StringSource implements TableDataSource
    {
        byte[] data;

        public StringSource(byte[] data)
        {
            this.data = data;
        }

        @Override
        public String name()
        {
            return null;
        }

        @Override
        public long size()
        {
            return data.length;
        }

        @Override
        public ByteBuffer read(long offset, int length) throws IOException
        {
            return Slices.wrappedBuffer(data).copySlice((int) offset, length).toByteBuffer();
        }

        @Override
        public Callable<?> closer()
        {
            return new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    return null;
                }
            };
        }
    }

    private static class StringSink implements WritableFile
    {
        private ByteArrayOutputStream sb = new ByteArrayOutputStream();

        byte[] content;

        @Override
        public void append(Slice data) throws IOException
        {
            sb.write(data.getBytes());
        }

        @Override
        public void force() throws IOException
        {
            content = sb.toByteArray();
        }

        @Override
        public void close() throws IOException
        {
            content = sb.toByteArray();
            sb.close();
            sb = null;
        }
    }

    private void tableTest(int blockSize, int blockRestartInterval, BlockEntry... entries)
            throws IOException
    {
        tableTest(blockSize, blockRestartInterval, asList(entries));
    }

    private void tableTest(int blockSize, int blockRestartInterval, List<BlockEntry> entries)
            throws IOException
    {
        reopenFile();
        Options options = new Options().blockSize(blockSize).blockRestartInterval(blockRestartInterval);
        TableBuilder builder = new TableBuilder(options, FileChannelWritableFile.fileChannel(fileChannel), new BytewiseComparator());

        for (BlockEntry entry : entries) {
            builder.add(entry);
        }
        builder.finish();

        Table table = createTable(file.getAbsolutePath(), fileChannel, new BytewiseComparator(), true, null);

        SeekingIterator<Slice, Slice> seekingIterator = table.iterator();
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

        Slice endKey = Slices.wrappedBuffer(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
        seekingIterator.seek(endKey);
        BlockHelper.assertSequence(seekingIterator, Collections.<BlockEntry>emptyList());

        long approximateOffset = table.getApproximateOffsetOf(endKey);
        assertTrue(approximateOffset >= lastApproximateOffset);

    }

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        reopenFile();
    }

    private void reopenFile()
            throws IOException
    {
        file = File.createTempFile("table", ".db");
        file.delete();
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
