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

import com.google.common.collect.Lists;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.DbConstants;
import org.iq80.leveldb.impl.DbImpl;
import org.iq80.leveldb.impl.InternalEntry;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.InternalKeyComparator;
import org.iq80.leveldb.impl.MemTable;
import org.iq80.leveldb.impl.SeekingIterator;
import org.iq80.leveldb.impl.SeekingIteratorAdapter;
import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.util.AbstractSeekingIterator;
import org.iq80.leveldb.util.Closeables;
import org.iq80.leveldb.util.FileUtils;
import org.iq80.leveldb.util.LRUCache;
import org.iq80.leveldb.util.RandomInputFile;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.Slices;
import org.iq80.leveldb.util.Snappy;
import org.iq80.leveldb.util.TestUtils;
import org.iq80.leveldb.util.UnbufferedWritableFile;
import org.iq80.leveldb.util.WritableFile;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.util.Arrays.asList;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;
import static org.iq80.leveldb.util.TestUtils.asciiToSlice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public abstract class TableTest
{
    private File file;

    protected abstract Table createTable(File file, Comparator<Slice> comparator, boolean verifyChecksums, FilterPolicy filterPolicy)
            throws IOException;

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEmptyFile()
            throws Exception
    {
        createTable(file, new BytewiseComparator(), true, null);
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

    @Test
    public void testZeroRestartPointsInBlock()
    {
        Block entries = new Block(Slices.allocate(SIZE_OF_INT), new BytewiseComparator());

        BlockIterator iterator = entries.iterator();
        iterator.seekToFirst();
        assertFalse(iterator.hasNext());
        iterator.seekToLast();
        assertFalse(iterator.hasNext());
        iterator.seek(asciiToSlice("foo"));
        assertFalse(iterator.hasNext());

    }

    private static final class KVMap extends ConcurrentSkipListMap<Slice, Slice>
    {
        public KVMap(UserComparator useComparator)
        {
            super(new STLLessThan(useComparator));
        }

        void add(String key, Slice value)
        {
            put(asciiToSlice(key), value);
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
    public void testTableApproximateOffsetOfPlain() throws Exception
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

    private abstract static class Constructor implements AutoCloseable, Iterable<Map.Entry<Slice, Slice>>
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
            kvMap.put(asciiToSlice(key), value);
        }

        void add(String key, String value)
        {
            add(key, asciiToSlice(value));
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
            return table.getApproximateOffsetOf(asciiToSlice(key));
        }

        @Override
        public SeekingIterator<Slice, Slice> iterator()
        {
            return table.iterator();
        }
    }

    @DataProvider(name = "testArgs")
    public Object[][] testArgsProvider()
    {
        try {
            final ReverseDBComparator reverse = new ReverseDBComparator();
            return new Object[][] {
                    {newHarness(TableConstructor.class, null, 16)},
                    {newHarness(TableConstructor.class, null, 1)},
                    {newHarness(TableConstructor.class, null, 1024)},
                    {newHarness(TableConstructor.class, reverse, 16)},
                    {newHarness(TableConstructor.class, reverse, 1)},
                    {newHarness(TableConstructor.class, reverse, 1024)},

                    {newHarness(BlockConstructor.class, null, 16)},
                    {newHarness(BlockConstructor.class, null, 1)},
                    {newHarness(BlockConstructor.class, null, 1014)},
                    {newHarness(BlockConstructor.class, reverse, 16)},
                    {newHarness(BlockConstructor.class, reverse, 1)},
                    {newHarness(BlockConstructor.class, reverse, 1024)},

                    //TODO ported from original but need to be moved away. they don't exactly belong in current package!
                    {newHarness(MemTableConstructor.class, null, 16)},
                    {newHarness(MemTableConstructor.class, reverse, 16)},

                    {newHarness(DbConstructor.class, null, 16)},
                    {newHarness(DbConstructor.class, reverse, 16)},
            };
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Harness newHarness(Class<? extends Constructor> cls, DBComparator dbComparator, int restartInterval) throws Exception
    {
        Random rnd = new Random(301 + System.nanoTime());
        return new Harness(rnd, dbComparator, cls, restartInterval);
    }

    @Test(dataProvider = "testArgs")
    public void testEmpty(Harness harness) throws Exception
    {
        try {
            harness.test();
        }
        finally {
            harness.close();
        }
    }

    @Test(dataProvider = "testArgs")
    public void testSimpleEmptyKey(Harness harness) throws Exception
    {
        try {
            harness.add(Slices.EMPTY_SLICE, asciiToSlice("v"));
            harness.test();
        }
        finally {
            harness.close();
        }
    }

    @Test(dataProvider = "testArgs")
    public void testSimpleSingle(Harness harness) throws Exception
    {
        try {
            harness.add(asciiToSlice("abc"), asciiToSlice("v"));
            harness.test();
        }
        finally {
            harness.close();
        }
    }

    @Test(dataProvider = "testArgs")
    public void testSimpleMulti(Harness harness) throws Exception
    {
        try {
            harness.add(asciiToSlice("abc"), asciiToSlice("v"));
            harness.add(asciiToSlice("abcd"), asciiToSlice("v"));
            harness.add(asciiToSlice("ac"), asciiToSlice("v2"));
            harness.test();
        }
        finally {
            harness.close();
        }
    }

    @Test(dataProvider = "testArgs")
    public void testSimpleSpecialKey(Harness harness) throws Exception
    {
        try {
            harness.add(Slices.wrappedBuffer(new byte[] {-1, -1}), asciiToSlice("v3"));
            harness.test();
        }
        finally {
            harness.close();
        }
    }

    @Test(dataProvider = "testArgs")
    public void testRandomized(Harness harness) throws Exception
    {
        try {
            Random rnd = harness.getRnd();
            for (int numEntries = 0; numEntries < 2000;
                 numEntries += (numEntries < 50 ? 1 : 200)) {
                if ((numEntries % 10) == 0) {
                    //System.err.println(String.format("case %s: numEntries = %d", harness, numEntries));
                }
                for (int e = 0; e < numEntries; e++) {
                    harness.add(new Slice(TestUtils.randomKey(rnd, harness.getRandomSkewed(4))),
                            TestUtils.randomString(rnd, harness.getRandomSkewed(5)));
                }
            }
            harness.test();
        }
        finally {
            harness.close();
        }
    }

    @Test
    public void testRandomizedLongDB() throws Exception
    {
        Random rnd = new Random(301);
        try (Harness<DbConstructor> harness = new Harness<>(rnd, null, DbConstructor.class, 16)) {
            int numEntries = 100000;
            for (int e = 0; e < numEntries; e++) {
                harness.add(new Slice(TestUtils.randomKey(rnd, harness.getRandomSkewed(4))),
                        TestUtils.randomString(rnd, harness.getRandomSkewed(5)));
            }
            harness.test();
            // We must have created enough data to force merging
            int files = 0;
            for (int level = 0; level < DbConstants.NUM_LEVELS; level++) {
                files += Integer.valueOf(harness.constructor.db.getProperty("leveldb.num-files-at-level" + level));
            }
            assertTrue(files > 0);
        }
    }

    private static class Harness<T extends Constructor> implements AutoCloseable
    {
        private final UserComparator comparator;
        private String desc;
        private final Random rnd;
        private T constructor;
        private Options options;

        public Harness(Random random, DBComparator comparator, Class<T> cls, int restartInterval) throws Exception
        {
            this.rnd = random;
            this.options = new Options();
            options.blockRestartInterval(restartInterval);
            options.blockSize(256);
            if (comparator != null) {
                this.comparator = new CustomUserComparator(comparator);
                options.comparator(comparator);
            }
            else {
                this.comparator = new BytewiseComparator();
            }
            constructor = cls.getConstructor(UserComparator.class).newInstance(this.comparator);
            desc = cls.getSimpleName() + ", comparator= " + (comparator == null ? null : comparator.getClass().getSimpleName()) + ", restartInterval=" + restartInterval;
        }


        public Random getRnd()
        {
            return rnd;
        }

        public T getConstructor()
        {
            return constructor;
        }

        /**
         * Skewed: pick "base" uniformly from range [0,maxLog] and then
         * return "base" random bits.  The effect is to pick a number in the
         * range [0,2^maxLog-1] with exponential bias towards smaller numbers.
         **/
        private int getRandomSkewed(int maxLog)
        {
            return rnd.nextInt(Integer.MAX_VALUE) % (1 << rnd.nextInt(Integer.MAX_VALUE) % (maxLog + 1));
        }

        void add(Slice key, Slice value)
        {
            constructor.add(key, value);
        }

        private void testForwardScan(KVMap data)
        {
            SeekingIterator<Slice, Slice> iter = constructor.iterator();

            iter.seekToFirst();

            Iterator<Map.Entry<Slice, Slice>> iterator = data.entrySet().iterator();
            while (iter.hasNext()) {
                assertEqualsEntries(iter.next(), iterator.next());
            }
            if (iterator.hasNext()) {
                SeekingIterator<Slice, Slice> iterator1 = constructor.iterator();
                iterator1.seekToFirst();
                ArrayList<Map.Entry<Slice, Slice>> entries = Lists.newArrayList(iterator1);
                Map.Entry<Slice, Slice> next = iterator.next();
                assertFalse(iterator.hasNext());
            }
            assertFalse(iterator.hasNext());
        }

        private static void assertEqualsEntries(Map.Entry<Slice, Slice> actual, Map.Entry<Slice, Slice> expected)
        {
            assertEquals(actual.getKey(), expected.getKey());
            assertEquals(actual.getValue(), expected.getValue());
        }

        private void testRandomAccess(KVMap data)
        {
            SeekingIterator<Slice, Slice> iter = constructor.iterator();
            List<Slice> keys = Lists.newArrayList(data.keySet());

            //assertFalse(iter.hasNext());
            Iterator<Map.Entry<Slice, Slice>> modelIter = data.entrySet().iterator();
            for (int i = 0; i < 200; i++) {
                int toss = rnd.nextInt(5);
                switch (toss) {
                    case 0: {
                        if (iter.hasNext()) {
                            Map.Entry<Slice, Slice> itNex = iter.next();
                            Map.Entry<Slice, Slice> modelNex = modelIter.next();
                            assertEqualsEntries(itNex, modelNex);
                        }
                        break;
                    }

                    case 1: {
                        iter.seekToFirst();
                        modelIter = data.entrySet().iterator();
                        if (modelIter.hasNext()) {
                            Map.Entry<Slice, Slice> itNex = iter.next();
                            Map.Entry<Slice, Slice> modelNex = modelIter.next();
                            assertEqualsEntries(itNex, modelNex);
                        }
                        break;
                    }

                    case 2: {
                        modelIter = getEntryIterator(data, iter, keys);
                        break;
                    }

                    case 3: {
                        //TODO implement prev to all iterators
                    }
                    case 4: {
                        //TODO implement seekLast to all iterators
                        break;
                    }
                }
            }
        }

        private Iterator<Map.Entry<Slice, Slice>> getEntryIterator(KVMap data, SeekingIterator<Slice, Slice> iter, List<Slice> keys)
        {
            Iterator<Map.Entry<Slice, Slice>> modelIter;
            Slice key = pickRandomKey(rnd, keys);
            modelIter = data.tailMap(key).entrySet().iterator();
            iter.seek(key);
            if (modelIter.hasNext()) {
                Map.Entry<Slice, Slice> itNex = iter.next();
                Map.Entry<Slice, Slice> modelNex = modelIter.next();
                assertEqualsEntries(itNex, modelNex);
            }
            return modelIter;
        }

        Slice pickRandomKey(Random rnd, List<Slice> keys)
        {
            if (keys.isEmpty()) {
                return asciiToSlice("foo");
            }
            else {
                int index = rnd.nextInt(keys.size());
                Slice result = keys.get(index).copySlice();
                switch (rnd.nextInt(3)) {
                    case 0:
                        // Return an existing key
                        break;
                    case 1: {
                        // Attempt to return something smaller than an existing key
                        int idx1 = result.length() - 1;
                        if (result.length() > 0 && result.getByte(idx1) > '\0') {
                            result.setByte(idx1, result.getByte(idx1) - 1);
                        }
                        break;
                    }
                    case 2: {
                        // Return something larger than an existing key
                        result = increment(comparator, result);
                        break;
                    }
                }
                return result;
            }
        }

        Slice increment(Comparator cmp, Slice key)
        {
            Slice k;
            if (cmp instanceof BytewiseComparator) {
                k = key;
            }
            else {
                k = reverse(key);

            }
            byte[] bytes = Arrays.copyOf(k.getBytes(), k.length() + 1);
            bytes[k.length()] = 0;
            return new Slice(bytes);
        }

        private Slice reverse(Slice key)
        {
            byte[] bytes = new byte[key.length()];
            for (int i = 0, k = key.length() - 1; k >= 0; i++, k--) {
                bytes[i] = key.getByte(k);
            }
            return new Slice(bytes);
        }

        void test() throws IOException
        {
            KVMap data = constructor.finish(options);

            testForwardScan(data);
            //TODO TestBackwardScan(data);
            testRandomAccess(data);
        }

        @Override
        public void close() throws Exception
        {
            constructor.close();
        }

        @Override
        public String toString()
        {
            return desc;
        }
    }

    private static class BlockConstructor extends Constructor
    {
        private Block entries;

        public BlockConstructor(UserComparator comparator)
        {
            super(comparator);
        }

        @Override
        public SeekingIterator<Slice, Slice> iterator()
        {
            return entries.iterator();
        }

        @Override
        protected void finish(Options options, UserComparator cmp, KVMap map) throws IOException
        {
            BlockBuilder builder = new BlockBuilder(256, options.blockRestartInterval(), cmp);

            for (Map.Entry<Slice, Slice> entry : map.entrySet()) {
                builder.add(entry.getKey(), entry.getValue());
            }

            // Open the block
            Slice data = builder.finish();
            entries = new Block(data, cmp);
        }
    }

    private static class MemTableConstructor extends Constructor
    {


        private MemTable table;

        public MemTableConstructor(UserComparator comparator)
        {
            super(comparator);
        }

        @Override
        protected void finish(Options options, UserComparator comparator, KVMap kvMap) throws IOException
        {
            table = new MemTable(new InternalKeyComparator(comparator));
            int seq = 1;
            for (Map.Entry<Slice, Slice> e : kvMap.entrySet()) {
                table.add(seq++, ValueType.VALUE, e.getKey(), e.getValue());
            }
        }

        @Override
        public SeekingIterator<Slice, Slice> iterator()
        {
            return new AbstractSeekingIterator<Slice, Slice>()
            {
                MemTable.MemTableIterator iterator = table.iterator();

                @Override
                protected void seekToFirstInternal()
                {
                    iterator.seekToFirst();
                }

                @Override
                protected void seekInternal(Slice targetKey)
                {
                    iterator.seek(new InternalKey(targetKey, Integer.MAX_VALUE, ValueType.VALUE));
                }

                @Override
                protected Map.Entry<Slice, Slice> getNextElement()
                {
                    if (iterator.hasNext()) {
                        InternalEntry next = iterator.next();
                        return new AbstractMap.SimpleEntry<>(next.getKey().getUserKey(), next.getValue());
                    }
                    else {
                        return null;
                    }
                }
            };
        }
    }

    private static class DbConstructor extends Constructor
    {


        private DbImpl db;
        private File tmpDir;

        public DbConstructor(UserComparator comparator)
        {
            super(comparator);
        }

        @Override
        protected void finish(Options options, UserComparator comparator, KVMap kvMap) throws IOException
        {
            options
                    .createIfMissing(true)
                    .errorIfExists(true)
                    .writeBufferSize(10000);  // Something small to force merging
            tmpDir = FileUtils.createTempDir("leveldb");
            this.db = new DbImpl(options, tmpDir);
            for (Map.Entry<Slice, Slice> entry : kvMap.entrySet()) {
                db.put(entry.getKey().getBytes(), entry.getValue().getBytes());
            }

        }

        @Override
        public SeekingIterator<Slice, Slice> iterator()
        {
            return new AbstractSeekingIterator<Slice, Slice>()
            {
                SeekingIteratorAdapter iterator = db.iterator();

                @Override
                protected void seekToFirstInternal()
                {
                    iterator.seekToFirst();
                }

                @Override
                protected void seekInternal(Slice targetKey)
                {
                    iterator.seek(targetKey.getBytes());
                }

                @Override
                protected Map.Entry<Slice, Slice> getNextElement()
                {
                    if (iterator.hasNext()) {
                        SeekingIteratorAdapter.DbEntry next = iterator.next();
                        return new AbstractMap.SimpleEntry<>(next.getKeySlice(), next.getValueSlice());
                    }
                    else {
                        return null;
                    }
                }
            };
        }

        @Override
        public void close() throws Exception
        {
            super.close();
            Closeables.closeQuietly(db);
            FileUtils.deleteRecursively(tmpDir);
        }
    }

    public class ReverseDBComparator
            implements DBComparator
    {
        private final BytewiseComparator com = new BytewiseComparator();

        @Override
        public String name()
        {
            return "leveldb.ReverseBytewiseComparator";
        }

        @Override
        public byte[] findShortestSeparator(byte[] start, byte[] limit)
        {
            Slice s = reverseToSlice(start);
            Slice l = reverseToSlice(limit);
            return reverseB(com.findShortestSeparator(s, l).getBytes());
        }

        private Slice reverseToSlice(byte[] key)
        {
            return new Slice(reverseB(key));
        }

        private byte[] reverseB(byte[] key)
        {
            byte[] bytes = new byte[key.length];
            for (int i = 0, k = key.length - 1; k >= 0; i++, k--) {
                bytes[i] = key[k];
            }
            return bytes;
        }

        @Override
        public byte[] findShortSuccessor(byte[] key)
        {
            Slice s = reverseToSlice(key);
            return reverseB(com.findShortSuccessor(s).getBytes());
        }

        @Override
        public int compare(byte[] a, byte[] b)
        {
            return com.compare(reverseToSlice(a), reverseToSlice(b));
        }
    }

    private static class StringSource implements RandomInputFile
    {
        byte[] data;

        public StringSource(byte[] data)
        {
            this.data = data;
        }

        @Override
        public long size()
        {
            return data.length;
        }

        @Override
        public ByteBuffer read(long offset, int length)
        {
            return Slices.wrappedBuffer(data).copySlice((int) offset, length).toByteBuffer();
        }

        @Override
        public void close()
        {

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
        TableBuilder builder = new TableBuilder(options, UnbufferedWritableFile.open(file), new BytewiseComparator());

        for (BlockEntry entry : entries) {
            builder.add(entry);
        }
        builder.finish();

        Table table = createTable(file, new BytewiseComparator(), true, null);

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
        com.google.common.io.Files.touch(file);
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        file.delete();
    }
}
