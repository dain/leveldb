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

package org.iq80.leveldb.util;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.iq80.leveldb.impl.ReverseIterator;
import org.iq80.leveldb.impl.ReverseIterators;
import org.iq80.leveldb.impl.ReversePeekingIterator;
import org.iq80.leveldb.impl.ReverseSeekingIterator;
import org.iq80.leveldb.table.BlockHelper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

import static com.google.common.base.Charsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class DBIteratorTest
{
    private static final List<Entry<String, String>> entries, ordered, rOrdered;
    private final Options options = new Options().createIfMissing(true);
    private DB db;
    private File tempDir;

    static {
        Random rand = new Random(0);

        ArrayList<Entry<String, String>> e = new ArrayList<Entry<String, String>>();
        int items = 1000000;
        for (int i = 0; i < items; i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < rand.nextInt(10) + 20; j++) {
                sb.append((char) ('a' + rand.nextInt(26)));
            }
            String key = sb.toString();
            sb.insert(0, "V:");
            String val = sb.substring(0, rand.nextInt(10) + sb.length() - 10);
            e.add(Maps.immutableEntry(key, val));
        }
        entries = Collections.unmodifiableList(e);
        ordered = Collections.unmodifiableList(ordered(entries));
        rOrdered = Collections.unmodifiableList(reverseOrdered(entries));
    }

    @BeforeMethod
    protected void setUp()
            throws Exception
    {
        tempDir = FileUtils.createTempDir("java-leveldb-testing-temp");
        db = Iq80DBFactory.factory.open(tempDir, options);
    }

    @AfterMethod
    protected void tearDown()
            throws Exception
    {
        try {
            db.close();
        }
        finally {
            FileUtils.deleteRecursively(tempDir);
        }
    }

    private static List<Entry<String, String>> ordered(Collection<Entry<String, String>> c)
    {
        return Ordering.from(new StringDbIterator.EntryCompare()).sortedCopy(c);
    }

    private static List<Entry<String, String>> reverseOrdered(Collection<Entry<String, String>> c)
    {
        return Ordering.from(new StringDbIterator.ReverseEntryCompare()).sortedCopy(c);
    }

    @Test
    public void testStraightIteration()
    {
        putAll(db, entries);

        straightIteration(db, ordered, rOrdered);
    }

    @Test
    public void testStraightIterationWithDeletes()
    {
        putAll(db, entries);

        List<Entry<String, String>> preserved = deletePortion(db, ordered, 0.5);
        straightIteration(db, preserved, reverseOrdered(preserved));
    }

    private static void straightIteration(DB db, Iterable<Entry<String, String>> forward, Iterable<Entry<String, String>> backward)
    {
        StringDbIterator actual = new StringDbIterator(db.iterator());
        actual.seekToFirst();
        assertForwardSame(actual, forward);
        assertBackwardSame(actual, backward);

        actual.seekToEnd();
        assertBackwardSame(actual, backward);
        assertForwardSame(actual, forward);
    }

    @Test
    public void testSeeks()
            throws ExecutionException, InterruptedException
    {
        putAll(db, entries);

        System.out.println("Testing seeks (this may take a while)");
        doSeeks(db, ordered, rOrdered, 0.01, 0.001);
    }

    private void doSeeks(
            final DB db,
            final List<Entry<String, String>> expectedForward,
            final List<Entry<String, String>> expectedBackward,
            final double seekRate,
            final double seekCheck)
            throws ExecutionException, InterruptedException
    {
        doSeeks(db, expectedForward, expectedBackward, seekRate, seekCheck, new ReadOptions(), null);
    }

    /**
     * @param seekRate (approximate) portion [0.0, 1.0] of entries to seek to. too many slows the test down, too few limits the usefulness of the test
     * @param seekCheck portion [0.0, 1.0] of the entries to confirm match (don't traverse whole list because it's slow)
     */
    private void doSeeks(
            final DB db,
            final List<Entry<String, String>> expectedForward,
            final List<Entry<String, String>> expectedBackward,
            final double seekRate,
            final double seekCheck,
            final ReadOptions readOptions,
            final StringDbIterator dbiter)
            throws ExecutionException, InterruptedException
    {
        final int size = expectedForward.size();
        final int seekCheckDist = (int) (size * seekCheck);
        final int coreNum = Runtime.getRuntime().availableProcessors();
        ExecutorService pool =
                Executors.newFixedThreadPool(coreNum);
        List<Future<Void>> work = new ArrayList<Future<Void>>();
        Random rand = new Random(0);

        int i = 0;
        for (final Entry<String, String> e : expectedForward) {
            if (rand.nextDouble() < seekRate) {
                final int seekIndex = i;
                work.add(pool.submit(new Callable<Void>()
                {
                    @Override
                    public Void call()
                    {
                        StringDbIterator actual = dbiter != null ? dbiter : new StringDbIterator(db.iterator(readOptions));
                        int loc = seekIndex;
                        List<Entry<String, String>> forwardExpected =
                                expectedForward.subList(loc, Math.min(size, loc + seekCheckDist));
                        loc = expectedBackward.size() - seekIndex;
                        List<Entry<String, String>> reverseExpected =
                                expectedBackward.subList(loc, Math.min(size, loc + seekCheckDist));
                        actual.seek(e.getKey());
                        assertForwardSame(actual, forwardExpected, false);
                        actual.seek(e.getKey());
                        assertBackwardSame(actual, reverseExpected, false);
                        return null;
                    }
                }));
            }
            i++;
        }

        for (Future<Void> f : work) {
            f.get();
        }
    }

    @Test
    public void testSeeksWithConcurrentOps()
            throws ExecutionException, InterruptedException
    {
        putAll(db, entries);
        StringDbIterator actual = new StringDbIterator(db.iterator());

        int concurrency = 10;
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        List<Future<Void>> work = new ArrayList<Future<Void>>();
        for (int i = 0; i < concurrency; i++) {
            final int j = i;
            work.add(pool.submit(new Callable<Void>()
            {
                public Void call()
                        throws Exception
                {
                    Random rand = new Random(j);
                    while (!Thread.interrupted()) {
                        int r = rand.nextInt(entries.size() - 1);
                        String target = entries.get(r).getKey();
                        if (rand.nextDouble() < 0.5) {
                            put(db, target, "dummy val:" + r);
                        }
                        else {
                            delete(db, target);
                        }
                    }
                    return null;
                }
            }));
        }

        Random rand = new Random(0);
        int max = (int) (entries.size() * 0.005f);
        int dist = 1000;
        for (int i = 0; i < max; i++) {
            int index = rand.nextInt(ordered.size() - 1);
            String target = ordered.get(index).getKey();
            double r = rand.nextDouble();
            if (r < 1.0 / 3) {
                target = BlockHelper.beforeString(Maps.immutableEntry(target, null));
            }
            else if (r > 2.0 / 3) {
                index++;
                target = BlockHelper.afterString(Maps.immutableEntry(target, null));
            }
            actual.seek(target);
            assertForwardSame(actual, ordered.subList(index, Math.min(ordered.size(), index + dist)), false);
            actual.seek(target);
            assertBackwardSame(actual, rOrdered.subList(rOrdered.size() - index, Math.min(rOrdered.size(), rOrdered.size() - index + dist)), false);
        }

        for (Future<Void> job : work) {
            job.cancel(true);
        }
    }

    private static void assertForwardSame(StringDbIterator actual, Iterable<Entry<String, String>> expected)
    {
        assertForwardSame(actual, expected, true);
    }

    private static void assertForwardSame(
            StringDbIterator actual,
            Iterable<Entry<String, String>> expected,
            boolean endCheck)
    {
        int i = 0;
        for (Entry<String, String> expectedEntry : expected) {
            assertTrue(actual.hasNext());
            Entry<String, String> p = actual.peek();
            assertEquals(expectedEntry, p, "Item #" + i + " peek mismatch");
            Entry<String, String> n = actual.next();
            assertEquals(expectedEntry, n, "Item #" + i + " mismatch");
            i++;
        }
        if (endCheck) {
            assertFalse(actual.hasNext());

            try {
                actual.peek();
                fail("expected NoSuchElementException");
            }
            catch (NoSuchElementException ignore) {
            }
            try {
                actual.next();
                fail("expected NoSuchElementException");
            }
            catch (NoSuchElementException ignore) {
            }
        }
    }

    private static void assertBackwardSame(StringDbIterator actual, Iterable<Entry<String, String>> expected)
    {
        assertBackwardSame(actual, expected, true);
    }

    private static void assertBackwardSame(
            StringDbIterator actual,
            Iterable<Entry<String, String>> expected,
            boolean endCheck)
    {
        int i = 0;
        for (Entry<String, String> expectedEntry : expected) {
            assertTrue(actual.hasPrev());
            Entry<String, String> p = actual.peekPrev();
            assertEquals(expectedEntry, p, "Item #" + i + " peek mismatch");
            Entry<String, String> n = actual.prev();
            assertEquals(expectedEntry, n, "Item #" + i + " mismatch");
            i++;
        }
        if (endCheck) {
            assertFalse(actual.hasPrev());

            try {
                actual.peekPrev();
                fail("expected NoSuchElementException");
            }
            catch (NoSuchElementException ignore) {
            }
            try {
                actual.prev();
                fail("expected NoSuchElementException");
            }
            catch (NoSuchElementException ignore) {
            }
        }
    }

    @Test
    public void testIterationSnapshotWithSeeks()
            throws ExecutionException, InterruptedException, IOException
    {
        List<List<Entry<String, String>>> splitList = Lists.partition(entries, entries.size() / 2);
        List<Entry<String, String>> firstHalf = splitList.get(0), secondHalf = splitList.get(1);
        putAll(db, firstHalf);
        // TODO: use actual snapshots once snapshot preservation through compaction is supported
        //Snapshot snapshot = db.getSnapshot();
        StringDbIterator actual = new StringDbIterator(db.iterator());

        // delete one-third of the entries in the db
        deletePortion(db, firstHalf, 1.0 / 3.0);

        // put in another set of data
        putAll(db, secondHalf);

        List<Entry<String, String>> forward = ordered(firstHalf), backward =
                reverseOrdered(firstHalf);

        //ReadOptions snapshotRead = new ReadOptions().snapshot(snapshot);
        // snapshot should retain the entries of the first insertion batch
        //StringDbIterator actual = new StringDbIterator(db.iterator(snapshotRead));

        actual.seekToFirst();
        assertForwardSame(actual, forward);
        assertBackwardSame(actual, backward);

        actual.seekToEnd();
        assertBackwardSame(actual, backward);
        assertForwardSame(actual, forward);

        // TODO seeks after deletes require snapshots
        //System.out.println("Testing snapshot seeks (this may take a while)");
        //doSeeks(db, forward, backward, 0.01, 0.001, snapshotRead);
    }

    private static List<Entry<String, String>> deletePortion(DB db, Collection<Entry<String, String>> items, double portion)
    {
        LinkedList<Entry<String, String>> remaining = new LinkedList<Entry<String, String>>(items);
        Iterator<Entry<String, String>> iter = remaining.iterator();
        Random rand = new Random(0);
        while (iter.hasNext()) {
            Entry<String, String> entry = iter.next();
            if (rand.nextDouble() < portion) {
                delete(db, entry.getKey());
                iter.remove();
            }
        }
        return remaining;
    }

    @Test
    public void testSmallIterationSnapshot()
    {
        put(db, "a", "0");
        put(db, "b", "0");

        put(db, "d", "0");
        put(db, "e", "0");
        put(db, "f", "0");
        put(db, "g", "0");

        delete(db, "a");
        delete(db, "d");
        delete(db, "f");
        delete(db, "g");
        put(db, "a", "1");
        put(db, "e", "1");
        put(db, "g", "1");

        Snapshot snapshot = db.getSnapshot();

        put(db, "a", "2");
        put(db, "c", "2");
        put(db, "c", "3");
        put(db, "e", "2");
        put(db, "f", "2");

        String[] _expected = {"a1", "b0", "e1", "g1"};
        List<Entry<String, String>> expected = new ArrayList<>();
        for (String s : _expected) {
            expected.add(Maps.immutableEntry("" + s.charAt(0), "" + s.charAt(1)));
        }
        List<Entry<String, String>> reverseExpected = reverseOrdered(expected);
        StringDbIterator actual = new StringDbIterator(db.iterator(new ReadOptions().snapshot(snapshot)));

        actual.seekToFirst();
        assertForwardSame(actual, expected);
        assertBackwardSame(actual, reverseExpected);

        actual.seekToLast();
        actual.next();
        assertBackwardSame(actual, reverseExpected);
        assertForwardSame(actual, expected);
    }

    @Test
    public void testMixedIteration()
    {

        putAll(db, entries);

        ReversePeekingIterator<Entry<String, String>> expected =
                ReverseIterators.reversePeekingIterator(ordered);
        ReverseSeekingIterator<String, String> actual = new StringDbIterator(db.iterator());

        mixedIteration(entries.size(), actual, expected);
    }

    @Test
    public void testMixedIterationWithDeletes()
    {
        putAll(db, entries);

        List<Entry<String, String>> preserved = deletePortion(db, ordered, 0.75);

        mixedIteration(preserved.size(), new StringDbIterator(db.iterator()), ReverseIterators.reversePeekingIterator(preserved));
    }

    private static void mixedIteration(int size, ReverseSeekingIterator<String, String> actual, ReversePeekingIterator<Entry<String, String>> expected)
    {
        Random rand = new Random(0);
        // take mixed forward and backward steps up the list then down the list (favoring forward to
        // reach the end, then backward)
        int pos = 0;
        int randForward = 12, randBack = 4;// [-4, 7] inclusive, initially favor forward steps
        int steps = randForward + 1;
        do {
            int direction = steps < 0 ? -1 : 1; // mathematical sign for addition
            for (int i = 0; Math.abs(i) < Math.abs(steps); i += direction) {
                // if the expected iterator has items in this direction, proceed
                if (hasFollowing(direction, expected)) {
                    assertTrue(hasFollowing(direction, actual));

                    assertEquals(
                            peekFollowing(direction, expected),
                            peekFollowing(direction, actual),
                            "Item #" + pos + " mismatch");

                    assertEquals(
                            getFollowing(direction, expected),
                            getFollowing(direction, actual),
                            "Item #" + pos + " mismatch");

                    pos += direction;
                }
                else {
                    break;
                }
            }
            if (pos >= size) {
                // switch to favor backward steps
                randForward = 4;
                randBack = 12;
                // [-7, 4] inclusive
            }
            while ((steps = rand.nextInt(randForward) - randBack) == 0) {
                ;
            }
        }
        while (pos > 0);
    }

    @Test
    public void testSeekPastContentsWithDeletesAndReverse()
    {
        String keys[] = {"a", "b", "c", "d", "e", "f"};
        int deleteIndex[] = {2, 3, 5};

        List<Entry<String, String>> keyvals = new ArrayList<Entry<String, String>>();
        for (String key : keys) {
            keyvals.add(Maps.immutableEntry(key, "v" + key));
        }

        for (int i = 0, d = 0; i < keyvals.size(); i++) {
            Entry<String, String> e = keyvals.get(i);
            db.put(e.getKey().getBytes(UTF_8), e.getValue().getBytes(UTF_8));
            if (d < deleteIndex.length && i == deleteIndex[d]) {
                delete(db, e.getKey());
                d++;
            }
        }

        Set<Entry<String, String>> expectedSet =
                new TreeSet<Entry<String, String>>(new Comparator<Entry<String, String>>()
                {
                    public int compare(Entry<String, String> o1, Entry<String, String> o2)
                    {
                        return o1.getKey().compareTo(o2.getKey());
                    }
                });
        for (int i = deleteIndex.length - 1; i >= 0; i--) {
            keyvals.remove(deleteIndex[i]);
        }
        expectedSet.addAll(keyvals);
        List<Entry<String, String>> expected = new ArrayList<Entry<String, String>>(expectedSet);

        StringDbIterator actual = new StringDbIterator(db.iterator());
        actual.seek("f");
        for (int i = expected.size() - 1; i >= 0; i--) {
            assertTrue(actual.hasPrev());
            assertEquals(expected.get(i), actual.peekPrev());
            assertEquals(expected.get(i), actual.prev());
        }
        assertFalse(actual.hasPrev());

        actual.seek("g");
        assertFalse(actual.hasNext());
        for (int i = expected.size() - 1; i >= 0; i--) {
            assertTrue(actual.hasPrev());
            assertEquals(expected.get(i), actual.peekPrev());
            assertEquals(expected.get(i), actual.prev());
        }
        assertFalse(actual.hasPrev());

        // recreating a strange set of circumstances encountered in the field
        actual.seek("g");
        assertFalse(actual.hasNext());
        actual.seekToLast();
        List<Entry<String, String>> items = new ArrayList<Entry<String, String>>();
        Entry<String, String> peek = actual.peek();
        items.add(peek);
        assertEquals(expected.get(expected.size() - 1), peek);
        assertTrue(actual.hasPrev());
        Entry<String, String> prev = actual.prev();
        items.add(prev);
        assertEquals(expected.get(expected.size() - 2), prev);

        while (actual.hasPrev()) {
            items.add(actual.prev());
        }

        Collections.reverse(items);
        assertEquals(expected, items);
    }

    private static void putAll(DB db, Iterable<Entry<String, String>> entries)
    {
        for (Entry<String, String> e : entries) {
            put(db, e.getKey(), e.getValue());
        }
    }

    private static Snapshot putAllSnapshot(DB db, Iterable<Entry<String, String>> entries)
    {
        WriteBatch batch = db.createWriteBatch();
        for (Entry<String, String> e : entries) {
            batch.put(e.getKey().getBytes(UTF_8), e.getValue().getBytes(UTF_8));
        }
        return db.write(batch, writeOptions);
    }

    private final static WriteOptions writeOptions = new WriteOptions().snapshot(true);

    private static void put(DB db, String key, String val)
    {
        db.put(key.getBytes(UTF_8), val.getBytes(UTF_8));
    }

    private static void delete(DB db, String key)
    {
        db.delete(key.getBytes(UTF_8));
    }

    private static boolean hasFollowing(int direction, ReverseIterator<?> iter)
    {
        return direction < 0 ? iter.hasPrev() : iter.hasNext();
    }

    private static <T> T peekFollowing(int direction, ReversePeekingIterator<T> iter)
    {
        return direction < 0 ? iter.peekPrev() : iter.peek();
    }

    private static <T> T getFollowing(int direction, ReverseIterator<T> iter)
    {
        return direction < 0 ? iter.prev() : iter.next();
    }

    private static class StringDbIterator
            implements ReverseSeekingIterator<String, String>
    {
        private DBIterator iterator;

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
            return Maps.immutableEntry(new String(next.getKey(), UTF_8), new String(next.getValue(),
                    UTF_8));
        }

        @Override
        public Entry<String, String> peekPrev()
        {
            return adapt(iterator.peekPrev());
        }

        @Override
        public Entry<String, String> prev()
        {
            return adapt(iterator.prev());
        }

        @Override
        public boolean hasPrev()
        {
            return iterator.hasPrev();
        }

        @Override
        public void seekToLast()
        {
            iterator.seekToLast();
        }

        @Override
        public void seekToEnd()
        {
            iterator.seekToLast();
            iterator.next();
        }

        public static class EntryCompare
                implements Comparator<Entry<String, String>>
        {
            @Override
            public int compare(Entry<String, String> o1, Entry<String, String> o2)
            {
                return o1.getKey().compareTo(o2.getKey());
            }
        }

        public static class ReverseEntryCompare
                extends EntryCompare
        {
            @Override
            public int compare(Entry<String, String> o1, Entry<String, String> o2)
            {
                return -super.compare(o1, o2);
            }
        }
    }
}
