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
package org.iq80.leveldb.impl;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.primitives.UnsignedBytes;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.Range;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.table.BloomFilterPolicy;
import org.iq80.leveldb.util.FileUtils;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.Slices;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.collect.Maps.immutableEntry;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.iq80.leveldb.CompressionType.NONE;
import static org.iq80.leveldb.impl.DbConstants.NUM_LEVELS;
import static org.iq80.leveldb.table.BlockHelper.afterString;
import static org.iq80.leveldb.table.BlockHelper.assertSequence;
import static org.iq80.leveldb.table.BlockHelper.beforeString;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class DbImplTest
{
    // You can set the STRESS_FACTOR system property to make the tests run more iterations.
    public static final double STRESS_FACTOR = Double.parseDouble(System.getProperty("STRESS_FACTOR", "1"));

    private static final String DOES_NOT_EXIST_FILENAME = "/foo/bar/doowop/idontexist";
    private static final String DOES_NOT_EXIST_FILENAME_PATTERN = ".foo.bar.doowop.idontexist";

    private File databaseDir;

    @DataProvider(name = "options")
    public Object[][] optionsProvider()
    {
        return new Object[][] {
                {"No Compression", new Options().compressionType(CompressionType.NONE)},
                {"Bloom Filter", new Options().filterPolicy(new BloomFilterPolicy(10))},
                {"Snappy", new Options().compressionType(CompressionType.SNAPPY)}
        };
    }

    @Test(dataProvider = "options")
    public void testBackgroundCompaction(final String desc, final Options options)
            throws Exception
    {
        options.maxOpenFiles(100);
        options.createIfMissing(true);
        DbImpl db = new DbImpl(options, this.databaseDir);
        Random random = new Random(301);
        for (int i = 0; i < 200000 * STRESS_FACTOR; i++) {
            db.put(randomString(random, 64).getBytes(), new byte[] {0x01}, new WriteOptions().sync(false));
            db.get(randomString(random, 64).getBytes());
            if ((i % 50000) == 0 && i != 0) {
                System.out.println(i + " rows written");
            }
        }
    }

    @Test
    public void testConcurrentWrite() throws Exception
    {
        Options options = new Options();
        options.maxOpenFiles(50);
        options.createIfMissing(true);
        final DbImpl db = new DbImpl(options, this.databaseDir);
        ExecutorService ex = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4);
        try {
            final int numEntries = 1000000;
            final int growValueBy = 10;
            final CountDownLatch segmentsToPutEnd = new CountDownLatch(numEntries / 100);
            final Random random = new Random(Thread.currentThread().getId());
            final int segmentSize = 100;
            //dispatch writes
            for (int i = 0; i < numEntries; i += segmentSize) {
                final int finalI = i;
                ex.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        final int i2 = finalI + segmentSize;
                        for (int j = finalI; j < i2; j++) {
                            final BigInteger bigInteger = BigInteger.valueOf(j);
                            final byte[] value = bigInteger.toByteArray();
                            final byte[] bytes = new byte[growValueBy + value.length];
                            for (int k = 0; k < growValueBy; k += value.length) {
                                System.arraycopy(value, 0, bytes, k, value.length);
                            }
                            db.put(value, bytes);
                            if (random.nextInt(100) < 2) {
                                Thread.yield();
                            }
                        }
                        segmentsToPutEnd.countDown();
                    }
                });
            }
            segmentsToPutEnd.await();
            //check all writes have
            for (int i = 0; i < numEntries; i++) {
                final BigInteger bigInteger = BigInteger.valueOf(i);
                final byte[] value = bigInteger.toByteArray();
                final byte[] bytes = new byte[growValueBy + value.length];
                for (int k = 0; k < growValueBy; k += value.length) {
                    System.arraycopy(value, 0, bytes, k, value.length);
                }
                assertEquals(db.get(value), bytes);
            }
        }
        finally {
            db.close();
            ex.shutdownNow();
        }
    }

    @Test(dataProvider = "options")
    public void testCompactionsOnBigDataSet(final String desc, final Options options)
            throws Exception
    {
        options.createIfMissing(true);
        DbImpl db = new DbImpl(options, databaseDir);
        for (int index = 0; index < 5000000; index++) {
            String key = "Key LOOOOOOOOOOOOOOOOOONG KEY " + index;
            String value = "This is element " + index + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABZASDFASDKLFJASDFKJSDFLKSDJFLKJSDHFLKJHSDJFSDFHJASDFLKJSDF";
            db.put(key.getBytes(UTF_8), value.getBytes(UTF_8));
        }
    }

    @Test(dataProvider = "options")
    public void testEmpty(final String desc, final Options options)
            throws Exception
    {
        File databaseDir = this.databaseDir;
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        assertNull(db.get("foo"));
    }

    @Test(dataProvider = "options")
    public void testEmptyBatch(final String desc, final Options options)
            throws Exception
    {
        // open new db
        options.createIfMissing(true);
        DB db = new Iq80DBFactory().open(databaseDir, options);

        // write an empty batch
        WriteBatch batch = db.createWriteBatch();
        batch.close();
        db.write(batch);

        // close the db
        db.close();

        // reopen db
        new Iq80DBFactory().open(databaseDir, options);
    }

    @Test(dataProvider = "options")
    public void testReadWrite(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("foo", "v1");
        assertEquals(db.get("foo"), "v1");
        db.put("bar", "v2");
        db.put("foo", "v3");
        assertEquals(db.get("foo"), "v3");
        assertEquals(db.get("bar"), "v2");
    }

    @Test(dataProvider = "options")
    public void testPutDeleteGet(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("foo", "v1");
        assertEquals(db.get("foo"), "v1");
        db.put("foo", "v2");
        assertEquals(db.get("foo"), "v2");
        db.delete("foo");
        assertNull(db.get("foo"));
    }

    @Test(dataProvider = "options")
    public void testGetFromImmutableLayer(final String desc, final Options options)
            throws Exception
    {
        // create db with small write buffer
        DbStringWrapper db = new DbStringWrapper(options.writeBufferSize(100000), databaseDir);
        db.put("foo", "v1");
        assertEquals(db.get("foo"), "v1");

        // todo Block sync calls

        // Fill memtable
        db.put("k1", longString(100000, 'x'));
        // Trigger compaction
        db.put("k2", longString(100000, 'y'));
        assertEquals(db.get("foo"), "v1");

        // todo Release sync calls
    }

    @Test(dataProvider = "options")
    public void testGetFromVersions(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("foo", "v1");
        db.compactMemTable();
        assertEquals(db.get("foo"), "v1");
    }

    @Test(dataProvider = "options")
    public void testGetSnapshot(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);

        // Try with both a short key and a long key
        for (int i = 0; i < 2; i++) {
            String key = (i == 0) ? "foo" : longString(200, 'x');
            db.put(key, "v1");
            Snapshot s1 = db.getSnapshot();
            db.put(key, "v2");
            assertEquals(db.get(key), "v2");
            assertEquals(db.get(key, s1), "v1");

            db.compactMemTable();
            assertEquals(db.get(key), "v2");
            assertEquals(db.get(key, s1), "v1");
            s1.close();
        }
    }

    @Test(dataProvider = "options")
    public void testGetLevel0Ordering(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);

        // Check that we process level-0 files in correct order.  The code
        // below generates two level-0 files where the earlier one comes
        // before the later one in the level-0 file list since the earlier
        // one has a smaller "smallest" key.
        db.put("bar", "b");
        db.put("foo", "v1");
        db.compactMemTable();
        db.put("foo", "v2");
        db.compactMemTable();
        assertEquals(db.get("foo"), "v2");
    }

    @Test(dataProvider = "options")
    public void testGetOrderedByLevels(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("foo", "v1");
        db.compact("a", "z");
        assertEquals(db.get("foo"), "v1");
        db.put("foo", "v2");
        assertEquals(db.get("foo"), "v2");
        db.compactMemTable();
        assertEquals(db.get("foo"), "v2");
    }

    @Test(dataProvider = "options")
    public void testGetPicksCorrectFile(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("a", "va");
        db.compact("a", "b");
        db.put("x", "vx");
        db.compact("x", "y");
        db.put("f", "vf");
        db.compact("f", "g");

        assertEquals(db.get("a"), "va");
        assertEquals(db.get("f"), "vf");
        assertEquals(db.get("x"), "vx");

    }

    //TODO implement GetEncountersEmptyLevel

    @Test(dataProvider = "options")
    public void testEmptyIterator(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        SeekingIterator<String, String> iterator = db.iterator();

        iterator.seekToFirst();
        assertNoNextElement(iterator);

        iterator.seek("foo");
        assertNoNextElement(iterator);
    }

    @Test(dataProvider = "options")
    public void testIteratorSingle(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("a", "va");

        assertSequence(db.iterator(), immutableEntry("a", "va"));
    }

    @Test(dataProvider = "options")
    public void testIteratorMultiple(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("a", "va");
        db.put("b", "vb");
        db.put("c", "vc");

        SeekingIterator<String, String> iterator = db.iterator();
        assertSequence(iterator,
                immutableEntry("a", "va"),
                immutableEntry("b", "vb"),
                immutableEntry("c", "vc"));
        iterator.seekToFirst();
        assertSequence(iterator,
                immutableEntry("a", "va"),
                immutableEntry("b", "vb"),
                immutableEntry("c", "vc"));

        // Make sure iterator stays at snapshot
        db.put("a", "va2");
        db.put("a2", "va3");
        db.put("b", "vb2");
        db.put("c", "vc2");

        iterator.seekToFirst();
        assertSequence(iterator,
                immutableEntry("a", "va"),
                immutableEntry("b", "vb"),
                immutableEntry("c", "vc"));
    }

    @Test(dataProvider = "options")
    public void testIterSmallAndLargeMix(final String desc, final Options options)
            throws IOException
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("a", "va");
        db.put("b", Strings.repeat("b", 100000));
        db.put("c", "vc");
        db.put("d", Strings.repeat("d", 100000));
        db.put("e", Strings.repeat("e", 100000));
        SeekingIterator<String, String> iterator = db.iterator();
        assertSequence(iterator,
                immutableEntry("a", "va"),
                immutableEntry("b", Strings.repeat("b", 100000)),
                immutableEntry("c", "vc"),
                immutableEntry("d", Strings.repeat("d", 100000)),
                immutableEntry("e", Strings.repeat("e", 100000)));

    }

    @Test(dataProvider = "options")
    public void testIterMultiWithDelete(final String desc, final Options options)
            throws IOException
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("b", "vb");
        db.put("c", "vc");
        db.put("a", "va");
        db.delete("b");
        assertNull(db.get("b"));
        SeekingIterator<String, String> iterator = db.iterator();
        iterator.seek("c");
        assertSequence(iterator,
                immutableEntry("c", "vc")
        );
    }

    @Test(dataProvider = "options")
    public void testRecover(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("foo", "v1");
        db.put("baz", "v5");

        db.reopen();

        assertEquals(db.get("foo"), "v1");
        assertEquals(db.get("baz"), "v5");
        db.put("bar", "v2");
        db.put("foo", "v3");

        db.reopen();

        assertEquals(db.get("foo"), "v3");
        db.put("foo", "v4");
        assertEquals(db.get("foo"), "v4");
        assertEquals(db.get("bar"), "v2");
        assertEquals(db.get("baz"), "v5");

    }

    @Test(dataProvider = "options")
    public void testRecoveryWithEmptyLog(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("foo", "v1");
        db.put("foo", "v2");
        db.reopen();
        db.reopen();
        db.put("foo", "v3");
        db.reopen();
        assertEquals(db.get("foo"), "v3");
    }

    @Test(dataProvider = "options")
    public void testRecoverDuringMemtableCompaction(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options.writeBufferSize(1000000), databaseDir);

        // Trigger a long memtable compaction and reopen the database during it
        db.put("foo", "v1");                        // Goes to 1st log file
        db.put("big1", longString(10000000, 'x'));  // Fills memtable
        db.put("big2", longString(1000, 'y'));      // Triggers compaction
        db.put("bar", "v2");                       // Goes to new log file

        db.reopen();
        assertEquals(db.get("foo"), "v1");
        assertEquals(db.get("bar"), "v2");
        assertEquals(db.get("big1"), longString(10000000, 'x'));
        assertEquals(db.get("big2"), longString(1000, 'y'));

    }

    @Test(dataProvider = "options")
    public void testMinorCompactionsHappen(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options.writeBufferSize(10000), databaseDir);

        int n = 500;
        int startingNumTables = db.totalTableFiles();
        for (int i = 0; i < n; i++) {
            db.put(key(i), key(i) + longString(1000, 'v'));
        }
        int endingNumTables = db.totalTableFiles();
        assertTrue(endingNumTables > startingNumTables);

        for (int i = 0; i < n; i++) {
            assertEquals(db.get(key(i)), key(i) + longString(1000, 'v'));
        }
        db.compactMemTable();

        for (int i = 0; i < n; i++) {
            assertEquals(db.get(key(i)), key(i) + longString(1000, 'v'));
        }

        db.reopen();
        for (int i = 0; i < n; i++) {
            assertEquals(db.get(key(i)), key(i) + longString(1000, 'v'));
        }

    }

    @Test(dataProvider = "options")
    public void testRecoverWithLargeLog(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("big1", longString(200000, '1'));
        db.put("big2", longString(200000, '2'));
        db.put("small3", longString(10, '3'));
        db.put("small4", longString(10, '4'));
        assertEquals(db.numberOfFilesInLevel(0), 0);

        db.reopen(options.writeBufferSize(100000));
        assertEquals(db.numberOfFilesInLevel(0), 3);
        assertEquals(db.get("big1"), longString(200000, '1'));
        assertEquals(db.get("big2"), longString(200000, '2'));
        assertEquals(db.get("small3"), longString(10, '3'));
        assertEquals(db.get("small4"), longString(10, '4'));
        assertTrue(db.numberOfFilesInLevel(0) > 1);
    }

    @Test(dataProvider = "options")
    public void testCompactionsGenerateMultipleFiles(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options.writeBufferSize(100000000), databaseDir);

        // Write 8MB (80 values, each 100K)
        assertEquals(db.numberOfFilesInLevel(0), 0);
        assertEquals(db.numberOfFilesInLevel(1), 0);
        Random random = new Random(301);
        List<String> values = new ArrayList<>();
        for (int i = 0; i < 80; i++) {
            String value = randomString(random, 100 * 1024);
            db.put(key(i), value);
            values.add(value);
        }

        // Reopening moves updates to level-0
        db.reopen();
        assertTrue(db.numberOfFilesInLevel(0) > 0);
        assertEquals(db.numberOfFilesInLevel(1), 0);
        db.compactRange(0, "", key(100000));

        assertEquals(db.numberOfFilesInLevel(0), 0);
        assertTrue(db.numberOfFilesInLevel(1) > 0);
        for (int i = 0; i < 80; i++) {
            assertEquals(db.get(key(i)), values.get(i));
        }
    }

    @Test(dataProvider = "options")
    public void testRepeatedWritesToSameKey(final String desc, final Options options)
            throws Exception
    {
        options.writeBufferSize(100000);
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);

        // We must have at most one file per level except for level-0,
        // which may have up to kL0_StopWritesTrigger files.
        int maxFiles = NUM_LEVELS + DbConstants.L0_STOP_WRITES_TRIGGER;

        Random random = new Random(301);
        String value = randomString(random, 2 * options.writeBufferSize());
        for (int i = 0; i < 5 * maxFiles; i++) {
            db.put("key", value);
            assertTrue(db.totalTableFiles() < maxFiles);
        }

        db.close();
    }

    @Test(dataProvider = "options")
    public void testSparseMerge(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(new Options().compressionType(NONE), databaseDir);

        fillLevels(db, "A", "Z");

        // Suppose there is:
        //    small amount of data with prefix A
        //    large amount of data with prefix B
        //    small amount of data with prefix C
        // and that recent updates have made small changes to all three prefixes.
        // Check that we do not do a compaction that merges all of B in one shot.
        String value = longString(1000, 'x');
        db.put("A", "va");

        // Write approximately 100MB of "B" values
        for (int i = 0; i < 100000; i++) {
            String key = String.format("B%010d", i);
            db.put(key, value);
        }
        db.put("C", "vc");
        db.compactMemTable();
        db.compactRange(0, "A", "Z");

        // Make sparse update
        db.put("A", "va2");
        db.put("B100", "bvalue2");
        db.put("C", "vc2");
        db.compactMemTable();

        // Compactions should not cause us to create a situation where
        // a file overlaps too much data at the next level.
        assertTrue(db.getMaxNextLevelOverlappingBytes() <= 20 * 1048576);
        db.compactRange(0, "", "z");
        assertTrue(db.getMaxNextLevelOverlappingBytes() <= 20 * 1048576);
        db.compactRange(1, "", "z");
        assertTrue(db.getMaxNextLevelOverlappingBytes() <= 20 * 1048576);
    }

    @Test
    public void testApproximateSizes()
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(new Options().writeBufferSize(100000000).compressionType(NONE), databaseDir);

        assertBetween(db.size("", "xyz"), 0, 0);
        db.reopen();
        assertBetween(db.size("", "xyz"), 0, 0);

        // Write 8MB (80 values, each 100K)
        assertEquals(db.numberOfFilesInLevel(0), 0);
        int n = 80;
        Random random = new Random(301);
        for (int i = 0; i < n; i++) {
            db.put(key(i), randomString(random, 100000));
        }

        // 0 because GetApproximateSizes() does not account for memtable space
        assertBetween(db.size("", key(50)), 0, 0);

        // Check sizes across recovery by reopening a few times
        for (int run = 0; run < 3; run++) {
            db.reopen();

            for (int compactStart = 0; compactStart < n; compactStart += 10) {
                for (int i = 0; i < n; i += 10) {
                    assertBetween(db.size("", key(i)), 100000 * i, 100000 * i + 10000);
                    assertBetween(db.size("", key(i) + ".suffix"), 100000 * (i + 1), 100000 * (i + 1) + 10000);
                    assertBetween(db.size(key(i), key(i + 10)), 100000 * 10, 100000 * 10 + 10000);
                }
                assertBetween(db.size("", key(50)), 5000000, 5010000);
                assertBetween(db.size("", key(50) + ".suffix"), 5100000, 5110000);

                db.compactRange(0, key(compactStart), key(compactStart + 9));
            }

            assertEquals(db.numberOfFilesInLevel(0), 0);
            assertTrue(db.numberOfFilesInLevel(1) > 0);
        }
    }

    @Test
    public void testApproximateSizesMixOfSmallAndLarge()
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(new Options().compressionType(NONE), databaseDir);
        Random random = new Random(301);
        String big1 = randomString(random, 100000);
        db.put(key(0), randomString(random, 10000));
        db.put(key(1), randomString(random, 10000));
        db.put(key(2), big1);
        db.put(key(3), randomString(random, 10000));
        db.put(key(4), big1);
        db.put(key(5), randomString(random, 10000));
        db.put(key(6), randomString(random, 300000));
        db.put(key(7), randomString(random, 10000));

        // Check sizes across recovery by reopening a few times
        for (int run = 0; run < 3; run++) {
            db.reopen();

            assertBetween(db.size("", key(0)), 0, 0);
            assertBetween(db.size("", key(1)), 10000, 11000);
            assertBetween(db.size("", key(2)), 20000, 21000);
            assertBetween(db.size("", key(3)), 120000, 121000);
            assertBetween(db.size("", key(4)), 130000, 131000);
            assertBetween(db.size("", key(5)), 230000, 231000);
            assertBetween(db.size("", key(6)), 240000, 241000);
            assertBetween(db.size("", key(7)), 540000, 541000);
            assertBetween(db.size("", key(8)), 550000, 551000);

            assertBetween(db.size(key(3), key(5)), 110000, 111000);

            db.compactRange(0, key(0), key(100));
        }
    }

    @Test(dataProvider = "options")
    public void testIteratorPinsRef(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("foo", "hello");

        SeekingIterator<String, String> iterator = db.iterator();

        db.put("foo", "newvalue1");
        for (int i = 0; i < 100; i++) {
            db.put(key(i), key(i) + longString(100000, 'v'));
        }
        db.put("foo", "newvalue1");

        assertSequence(iterator, immutableEntry("foo", "hello"));
    }

    @Test(dataProvider = "options")
    public void testSnapshot(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("foo", "v1");
        Snapshot s1 = db.getSnapshot();
        db.put("foo", "v2");
        Snapshot s2 = db.getSnapshot();
        db.put("foo", "v3");
        Snapshot s3 = db.getSnapshot();

        db.put("foo", "v4");

        assertEquals("v1", db.get("foo", s1));
        assertEquals("v2", db.get("foo", s2));
        assertEquals("v3", db.get("foo", s3));
        assertEquals("v4", db.get("foo"));

        s3.close();
        assertEquals("v1", db.get("foo", s1));
        assertEquals("v2", db.get("foo", s2));
        assertEquals("v4", db.get("foo"));

        s1.close();
        assertEquals("v2", db.get("foo", s2));
        assertEquals("v4", db.get("foo"));

        s2.close();
        assertEquals("v4", db.get("foo"));
    }

    @Test(dataProvider = "options")
    public void testHiddenValuesAreRemoved(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        Random random = new Random(301);
        fillLevels(db, "a", "z");

        String big = randomString(random, 50000);
        db.put("foo", big);
        db.put("pastFoo", "v");

        Snapshot snapshot = db.getSnapshot();

        db.put("foo", "tiny");
        db.put("pastFoo2", "v2");  // Advance sequence number one more

        db.compactMemTable();
        assertTrue(db.numberOfFilesInLevel(0) > 0);

        assertEquals(big, db.get("foo", snapshot));
        assertBetween(db.size("", "pastFoo"), 50000, 60000);
        snapshot.close();
        assertEquals(db.allEntriesFor("foo"), asList("tiny", big));
        db.compactRange(0, "", "x");
        assertEquals(db.allEntriesFor("foo"), asList("tiny"));
        assertEquals(db.numberOfFilesInLevel(0), 0);
        assertTrue(db.numberOfFilesInLevel(1) >= 1);
        db.compactRange(1, "", "x");
        assertEquals(db.allEntriesFor("foo"), asList("tiny"));

        assertBetween(db.size("", "pastFoo"), 0, 1000);
    }

    @Test
    public void testDeleteEntriesShouldNotAbeamOnIteration() throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(new Options().createIfMissing(true), databaseDir);
        db.put("b", "v");
        db.delete("b");
        db.delete("a");
        assertEquals("[]", Iterators.toString(db.iterator()));
    }

    @Test
    public void testL0CompactionGoogleBugIssue44a() throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(new Options().createIfMissing(true), databaseDir);
        db.reopen();
        db.put("b", "v");
        db.reopen();
        db.delete("b");
        db.delete("a");
        db.reopen();
        db.delete("a");
        db.reopen();
        db.put("a", "v");
        db.reopen();
        db.reopen();
        assertEquals("[a=v]", Iterators.toString(db.iterator()));
        Thread.sleep(1000);  // Wait for compaction to finish
        assertEquals("[a=v]", Iterators.toString(db.iterator()));
    }

    @Test
    public void testL0CompactionGoogleBugIssue44b() throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(new Options().createIfMissing(true), databaseDir);
        db.reopen();
        db.put("", "");
        db.reopen();
        db.delete("e");
        db.put("", "");
        db.reopen();
        db.put("c", "cv");
        db.reopen();
        assertEquals("[=, c=cv]", Iterators.toString(db.iterator()));
        db.put("", "");
        db.reopen();
        db.put("", "");
        Thread.sleep(1000);  // Wait for compaction to finish
        db.reopen();
        db.put("d", "dv");
        db.reopen();
        db.put("", "");
        db.reopen();
        db.delete("d");
        db.delete("b");
        db.reopen();
        assertEquals("[=, c=cv]", Iterators.toString(db.iterator()));
        Thread.sleep(1000);  // Wait for compaction to finish
        assertEquals("[=, c=cv]", Iterators.toString(db.iterator()));
    }

    @Test(dataProvider = "options")
    public void testDeletionMarkers1(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);

        db.put("foo", "v1");
        db.compactMemTable();

        int last = DbConstants.MAX_MEM_COMPACT_LEVEL;
        assertEquals(db.numberOfFilesInLevel(last), 1); // foo => v1 is now in last level

        // Place a table at level last-1 to prevent merging with preceding mutation
        db.put("a", "begin");
        db.put("z", "end");
        db.compactMemTable();
        assertEquals(db.numberOfFilesInLevel(last), 1);
        assertEquals(db.numberOfFilesInLevel(last - 1), 1);
        assertEquals(db.get("a"), "begin");
        assertEquals(db.get("foo"), "v1");
        assertEquals(db.get("z"), "end");

        db.delete("foo");
        db.put("foo", "v2");
        final List<String> foo = db.allEntriesFor("foo");
        assertEquals(foo, asList("v2", "DEL", "v1"));
        db.compactMemTable();  // Moves to level last-2
        assertEquals(db.get("a"), "begin");
        assertEquals(db.get("foo"), "v2");
        assertEquals(db.get("z"), "end");

        assertEquals(db.allEntriesFor("foo"), asList("v2", "DEL", "v1"));
        db.compactRange(last - 2, "", "z");

        // DEL eliminated, but v1 remains because we aren't compacting that level
        // (DEL can be eliminated because v2 hides v1).
        assertEquals(db.allEntriesFor("foo"), asList("v2", "v1"));
        db.compactRange(last - 1, "", "z");

        // Merging last-1 w/ last, so we are the base level for "foo", so
        // DEL is removed.  (as is v1).
        assertEquals(db.allEntriesFor("foo"), asList("v2"));
    }

    @Test(dataProvider = "options")
    public void testDeletionMarkers2(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);

        db.put("foo", "v1");
        db.compactMemTable();

        int last = DbConstants.MAX_MEM_COMPACT_LEVEL;
        assertEquals(db.numberOfFilesInLevel(last), 1); // foo => v1 is now in last level

        // Place a table at level last-1 to prevent merging with preceding mutation
        db.put("a", "begin");
        db.put("z", "end");
        db.compactMemTable();
        assertEquals(db.numberOfFilesInLevel(last), 1);
        assertEquals(db.numberOfFilesInLevel(last - 1), 1);

        db.delete("foo");

        assertEquals(db.allEntriesFor("foo"), asList("DEL", "v1"));
        db.compactMemTable();  // Moves to level last-2
        assertEquals(db.allEntriesFor("foo"), asList("DEL", "v1"));
        db.compactRange(last - 2, "", "z");

        // DEL kept: "last" file overlaps
        assertEquals(db.allEntriesFor("foo"), asList("DEL", "v1"));
        db.compactRange(last - 1, "", "z");

        // Merging last-1 w/ last, so we are the base level for "foo", so
        // DEL is removed.  (as is v1).
        assertEquals(db.allEntriesFor("foo"), asList());
    }

    @Test(dataProvider = "options")
    public void testEmptyDb(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        testDb(db);
    }

    @Test(dataProvider = "options")
    public void testSingleEntrySingle(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        testDb(db, immutableEntry("name", "dain sundstrom"));
    }

    @Test(dataProvider = "options")
    public void testMultipleEntries(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);

        List<Entry<String, String>> entries = asList(
                immutableEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                immutableEntry("beer/ipa", "Lagunitas IPA"),
                immutableEntry("beer/stout", "Lagunitas Imperial Stout"),
                immutableEntry("scotch/light", "Oban 14"),
                immutableEntry("scotch/medium", "Highland Park"),
                immutableEntry("scotch/strong", "Lagavulin"));

        testDb(db, entries);
    }

    @Test(dataProvider = "options")
    public void testMultiPassMultipleEntries(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);

        List<Entry<String, String>> entries = asList(
                immutableEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                immutableEntry("beer/ipa", "Lagunitas IPA"),
                immutableEntry("beer/stout", "Lagunitas Imperial Stout"),
                immutableEntry("scotch/light", "Oban 14"),
                immutableEntry("scotch/medium", "Highland Park"),
                immutableEntry("scotch/strong", "Lagavulin"));

        for (int i = 1; i < entries.size(); i++) {
            testDb(db, entries);
        }
    }

    //TODO this test may fail in windows. a path that also fails in windows must be found
    @Test(enabled = false, expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Database directory '" + DOES_NOT_EXIST_FILENAME_PATTERN + "'.*")
    public void testCantCreateDirectoryReturnMessage()
            throws Exception
    {
        new DbStringWrapper(new Options(), new File(DOES_NOT_EXIST_FILENAME));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Database directory.*is not a directory")
    public void testDBDirectoryIsFileRetrunMessage()
            throws Exception
    {
        File databaseFile = new File(databaseDir + "/imafile");
        assertTrue(databaseFile.createNewFile());
        new DbStringWrapper(new Options(), databaseFile);
    }

    @Test
    public void testSymbolicLinkForFileWithoutParent()
    {
        assertFalse(FileUtils.isSymbolicLink(new File("db")));
    }

    @Test
    public void testSymbolicLinkForFileWithParent()
    {
        assertFalse(FileUtils.isSymbolicLink(new File(DOES_NOT_EXIST_FILENAME, "db")));
    }

    @Test(dataProvider = "options")
    public void testCustomComparator(final String desc, final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options.comparator(new LexicographicalReverseDBComparator()), databaseDir);

        List<Entry<String, String>> entries = asList(
                immutableEntry("scotch/strong", "Lagavulin"),
                immutableEntry("scotch/medium", "Highland Park"),
                immutableEntry("scotch/light", "Oban 14"),
                immutableEntry("beer/stout", "Lagunitas Imperial Stout"),
                immutableEntry("beer/ipa", "Lagunitas IPA"),
                immutableEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’")
        );

        for (Entry<String, String> entry : entries) {
            db.put(entry.getKey(), entry.getValue());
        }

        SeekingIterator<String, String> seekingIterator = db.iterator();
        for (Entry<String, String> entry : entries) {
            assertTrue(seekingIterator.hasNext());
            assertEquals(seekingIterator.peek(), entry);
            assertEquals(seekingIterator.next(), entry);
        }

        assertFalse(seekingIterator.hasNext());
    }

    @Test(dataProvider = "options")
    public void testManualCompaction(final String desc, final Options options) throws Exception
    {
        assertEquals(DbConstants.MAX_MEM_COMPACT_LEVEL, 2);
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        makeTables(db, 3, "p", "q");
        assertEquals("1,1,1", filesPerLevel(db.db));

        // Compaction range falls before files
        db.compactRange("", "c");
        assertEquals("1,1,1", filesPerLevel(db.db));

        // Compaction range falls after files
        db.compactRange("r", "z");
        assertEquals("1,1,1", filesPerLevel(db.db));

        // Compaction range overlaps files
        db.compactRange("p1", "p9");
        assertEquals("0,0,1", filesPerLevel(db.db));

        // Populate a different range
        makeTables(db, 3, "c", "e");
        assertEquals("1,1,2", filesPerLevel(db.db));

        // Compact just the new range
        db.compactRange("b", "f");
        assertEquals("0,0,2", filesPerLevel(db.db));

        // Compact all
        makeTables(db, 1, "a", "z");
        assertEquals("0,1,2", filesPerLevel(db.db));
        db.compactRange(null, null);
        assertEquals("0,0,1", filesPerLevel(db.db));
    }

    // Do n memtable compactions, each of which produces an sstable
    // covering the range [small,large].
    private void makeTables(DbStringWrapper db, int n, String small, String large)
    {
        for (int i = 0; i < n; i++) {
            db.put(small, "begin");
            db.put(large, "end");
            db.db.testCompactMemTable();
        }
    }

    // Return spread of files per level
    private String filesPerLevel(DbImpl db)
    {
        StringBuilder result = new StringBuilder();
        int lastNonZeroOffset = 0;
        for (int level = 0; level < DbConstants.NUM_LEVELS; level++) {
            int f = db.numberOfFilesInLevel(level);
            if (result.length() > 0) {
                result.append(",");
            }
            result.append(f);
            if (f > 0) {
                lastNonZeroOffset = result.length();
            }
        }
        result.setLength(lastNonZeroOffset);
        return result.toString();
    }

    @SafeVarargs
    private final void testDb(DbStringWrapper db, Entry<String, String>... entries)
            throws IOException
    {
        testDb(db, asList(entries));
    }

    private void testDb(DbStringWrapper db, List<Entry<String, String>> entries)
            throws IOException
    {
        for (Entry<String, String> entry : entries) {
            db.put(entry.getKey(), entry.getValue());
        }

        for (Entry<String, String> entry : entries) {
            String actual = db.get(entry.getKey());
            assertEquals(actual, entry.getValue(), "Key: " + entry.getKey());
        }

        SeekingIterator<String, String> seekingIterator = db.iterator();
        assertSequence(seekingIterator, entries);

        seekingIterator.seekToFirst();
        assertSequence(seekingIterator, entries);

        for (Entry<String, String> entry : entries) {
            List<Entry<String, String>> nextEntries = entries.subList(entries.indexOf(entry), entries.size());
            seekingIterator.seek(entry.getKey());
            assertSequence(seekingIterator, nextEntries);

            seekingIterator.seek(beforeString(entry));
            assertSequence(seekingIterator, nextEntries);

            seekingIterator.seek(afterString(entry));
            assertSequence(seekingIterator, nextEntries.subList(1, nextEntries.size()));
        }

        Slice endKey = Slices.wrappedBuffer(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
        seekingIterator.seek(endKey.toString(UTF_8));
        assertSequence(seekingIterator, Collections.<Entry<String, String>>emptyList());
    }

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        databaseDir = FileUtils.createTempDir("leveldb");
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        for (DbStringWrapper db : opened) {
            db.close();
        }
        opened.clear();
        FileUtils.deleteRecursively(databaseDir);
    }

    private void assertBetween(long actual, int smallest, int greatest)
    {
        if (!between(actual, smallest, greatest)) {
            fail(String.format("Expected: %s to be between %s and %s", actual, smallest, greatest));
        }
    }

    private void assertNoNextElement(SeekingIterator<String, String> iterator)
    {
        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("Expected NoSuchElementException");
        }
        catch (NoSuchElementException expected) {
        }
        try {
            iterator.peek();
            fail("Expected NoSuchElementException");
        }
        catch (NoSuchElementException expected) {
        }
    }

    static byte[] toByteArray(String value)
    {
        return value.getBytes(UTF_8);
    }

    private static String randomString(Random random, int length)
    {
        char[] chars = new char[length];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = (char) ((int) ' ' + random.nextInt(95));
        }
        return new String(chars);

    }

    private static String longString(int length, char character)
    {
        char[] chars = new char[length];
        Arrays.fill(chars, character);
        return new String(chars);
    }

    public static String key(int i)
    {
        return String.format("key%06d", i);
    }

    private boolean between(long size, long left, long right)
    {
        return left <= size && size <= right;
    }

    private void fillLevels(DbStringWrapper db, String smallest, String largest)
    {
        makeTables(db, NUM_LEVELS, smallest, largest);
    }

    private final ArrayList<DbStringWrapper> opened = new ArrayList<>();

    private static class LexicographicalReverseDBComparator
            implements DBComparator
    {
        @Override
        public String name()
        {
            return "test";
        }

        @Override
        public int compare(byte[] sliceA, byte[] sliceB)
        {
            // reverse order
            return -(UnsignedBytes.lexicographicalComparator().compare(sliceA, sliceB));
        }

        @Override
        public byte[] findShortestSeparator(byte[] start, byte[] limit)
        {
            // Find length of common prefix
            int sharedBytes = calculateSharedBytes(start, limit);

            // Do not shorten if one string is a prefix of the other
            if (sharedBytes < Math.min(start.length, limit.length)) {
                // if we can add one to the last shared byte without overflow and the two keys differ by more than
                // one increment at this location.
                int lastSharedByte = start[sharedBytes] & 0xff;
                if (lastSharedByte < 0xff && lastSharedByte + 1 < limit[sharedBytes]) {
                    byte[] result = Arrays.copyOf(start, sharedBytes + 1);
                    result[sharedBytes] = (byte) (lastSharedByte + 1);

                    assert (compare(result, limit) < 0) : "start must be less than last limit";
                    return result;
                }
            }
            return start;
        }

        @Override
        public byte[] findShortSuccessor(byte[] key)
        {
            // Find first character that can be incremented
            for (int i = 0; i < key.length; i++) {
                int b = key[i];
                if (b != 0xff) {
                    byte[] result = Arrays.copyOf(key, i + 1);
                    result[i] = (byte) (b + 1);
                    return result;
                }
            }
            // key is a run of 0xffs.  Leave it alone.
            return key;
        }

        private int calculateSharedBytes(byte[] leftKey, byte[] rightKey)
        {
            int sharedKeyBytes = 0;

            if (leftKey != null && rightKey != null) {
                int minSharedKeyBytes = Math.min(leftKey.length, rightKey.length);
                while (sharedKeyBytes < minSharedKeyBytes && leftKey[sharedKeyBytes] == rightKey[sharedKeyBytes]) {
                    sharedKeyBytes++;
                }
            }

            return sharedKeyBytes;
        }
    }

    private class DbStringWrapper
    {
        private final Options options;
        private final File databaseDir;
        private DbImpl db;

        private DbStringWrapper(Options options, File databaseDir)
                throws IOException
        {
            this.options = options.verifyChecksums(true).createIfMissing(true).errorIfExists(true);
            this.databaseDir = databaseDir;
            this.db = new DbImpl(options, databaseDir);
            opened.add(this);
        }

        public String get(String key)
        {
            byte[] slice = db.get(toByteArray(key));
            if (slice == null) {
                return null;
            }
            return new String(slice, UTF_8);
        }

        public String get(String key, Snapshot snapshot)
        {
            byte[] slice = db.get(toByteArray(key), new ReadOptions().snapshot(snapshot));
            if (slice == null) {
                return null;
            }
            return new String(slice, UTF_8);
        }

        public void put(String key, String value)
        {
            db.put(toByteArray(key), toByteArray(value));
        }

        public void delete(String key)
        {
            db.delete(toByteArray(key));
        }

        public SeekingIterator<String, String> iterator()
        {
            return new StringDbIterator(db.iterator());
        }

        public Snapshot getSnapshot()
        {
            return db.getSnapshot();
        }

        public void close()
        {
            db.close();
        }

        public void compactMemTable()
        {
            db.flushMemTable();
        }

        public void compactRange(String start, String limit)
        {
            db.compactRange(start == null ? null : Slices.copiedBuffer(start, UTF_8).getBytes(), limit == null ? null : Slices.copiedBuffer(limit, UTF_8).getBytes());
        }

        public void compactRange(int level, String start, String limit)
        {
            db.compactRange(level, start == null ? null : Slices.copiedBuffer(start, UTF_8), limit == null ? null : Slices.copiedBuffer(limit, UTF_8));
        }

        public void compact(String start, String limit)
        {
            db.flushMemTable();
            int maxLevelWithFiles = 1;
            for (int level = 2; level < NUM_LEVELS; level++) {
                if (db.numberOfFilesInLevel(level) > 0) {
                    maxLevelWithFiles = level;
                }
            }
            for (int level = 0; level < maxLevelWithFiles; level++) {
                db.compactRange(level, Slices.copiedBuffer("", UTF_8), Slices.copiedBuffer("~", UTF_8));
            }

        }

        public int numberOfFilesInLevel(int level)
        {
            return db.numberOfFilesInLevel(level);
        }

        public int totalTableFiles()
        {
            int result = 0;
            for (int level = 0; level < NUM_LEVELS; level++) {
                result += db.numberOfFilesInLevel(level);
            }
            return result;
        }

        public long size(String start, String limit)
        {
            return db.getApproximateSizes(new Range(toByteArray(start), toByteArray(limit)));
        }

        public long getMaxNextLevelOverlappingBytes()
        {
            return db.getMaxNextLevelOverlappingBytes();
        }

        public void reopen()
                throws IOException
        {
            reopen(options);
        }

        public void reopen(Options options)
                throws IOException
        {
            db.close();
            db = new DbImpl(options.verifyChecksums(true).createIfMissing(false).errorIfExists(false), databaseDir);
        }

        private List<String> allEntriesFor(String userKey)
        {
            ImmutableList.Builder<String> result = ImmutableList.builder();
            for (Entry<InternalKey, Slice> entry : db.internalIterable()) {
                String entryKey = entry.getKey().getUserKey().toString(UTF_8);
                if (entryKey.equals(userKey)) {
                    if (entry.getKey().getValueType() == ValueType.VALUE) {
                        result.add(entry.getValue().toString(UTF_8));
                    }
                    else {
                        result.add("DEL");
                    }
                }
            }
            return result.build();
        }

    }

    private static class StringDbIterator
            implements SeekingIterator<String, String>
    {
        private final DBIterator iterator;

        private StringDbIterator(DBIterator iterator)
        {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public void seekToFirst()
        {
            iterator.seekToFirst();
        }

        @Override
        public void seek(String targetKey)
        {
            iterator.seek(targetKey.getBytes(UTF_8));
        }

        @Override
        public Entry<String, String> peek()
        {
            return adapt(iterator.peekNext());
        }

        @Override
        public Entry<String, String> next()
        {
            return adapt(iterator.next());
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        private Entry<String, String> adapt(Entry<byte[], byte[]> next)
        {
            return immutableEntry(new String(next.getKey(), UTF_8), new String(next.getValue(), UTF_8));
        }
    }
}
