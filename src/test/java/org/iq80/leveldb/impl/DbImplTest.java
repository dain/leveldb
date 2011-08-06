package org.iq80.leveldb.impl;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import org.iq80.leveldb.SeekingIterator;
import org.iq80.leveldb.table.BlockEntry;
import org.iq80.leveldb.table.Options;
import org.iq80.leveldb.util.FileUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import static com.google.common.base.Charsets.UTF_8;
import static java.util.Arrays.asList;
import static org.iq80.leveldb.table.BlockHelper.after;
import static org.iq80.leveldb.table.BlockHelper.assertSequence;
import static org.iq80.leveldb.table.BlockHelper.before;
import static org.testng.Assert.assertEquals;

public class DbImplTest
{
    private File databaseDir;

    @Test
    public void testEmptyBlock()
            throws Exception
    {
        DbImpl db = new DbImpl(new Options(), databaseDir);

        testDb(db);
    }


    @Test
    public void testSingleEntrySingle()
            throws Exception
    {
        DbImpl db = new DbImpl(new Options(), databaseDir);
        testDb(db, createEntry("name", "dain sundstrom"));
    }

    @Test
    public void testMultipleEntries()
            throws Exception
    {
        DbImpl db = new DbImpl(new Options().setWriteBufferSize(100), databaseDir);

        List<Entry<ChannelBuffer, ChannelBuffer>> entries = Arrays.asList(
                createEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                createEntry("beer/ipa", "Lagunitas IPA"),
                createEntry("beer/stout", "Lagunitas Imperial Stout"),
                createEntry("scotch/light", "Oban 14"),
                createEntry("scotch/medium", "Highland Park"),
                createEntry("scotch/strong", "Lagavulin"));

        testDb(db, entries);
    }

    @Test
    public void testMultiPassMultipleEntries()
            throws Exception
    {
        DbImpl db = new DbImpl(new Options().setWriteBufferSize(100), databaseDir);

        List<Entry<ChannelBuffer, ChannelBuffer>> entries = Arrays.asList(
                createEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                createEntry("beer/ipa", "Lagunitas IPA"),
                createEntry("beer/stout", "Lagunitas Imperial Stout"),
                createEntry("scotch/light", "Oban 14"),
                createEntry("scotch/medium", "Highland Park"),
                createEntry("scotch/strong", "Lagavulin"));

        for (int i = 1; i < entries.size(); i++) {
            testDb(db, entries);
        }
    }

    private void testDb(DbImpl db, Entry<ChannelBuffer, ChannelBuffer>... entries)
            throws IOException
    {
        testDb(db, asList(entries));
    }

    private void testDb(DbImpl db, List<Entry<ChannelBuffer, ChannelBuffer>> entries)
            throws IOException
    {

        for (Entry<ChannelBuffer, ChannelBuffer> entry : entries) {
            db.put(entry.getKey(), entry.getValue());
        }

        for (Entry<ChannelBuffer, ChannelBuffer> entry : entries) {
            ChannelBuffer actual = db.get(entry.getKey());
            assertEquals(actual, entry.getValue(), "Key: " +  entry.getKey().toString(UTF_8));
        }

        SeekingIterator<ChannelBuffer, ChannelBuffer> seekingIterator = db.iterator();
        assertSequence(seekingIterator, entries);

        seekingIterator.seekToFirst();
        assertSequence(seekingIterator, entries);

        for (Entry<ChannelBuffer, ChannelBuffer> entry : entries) {
            List<Entry<ChannelBuffer, ChannelBuffer>> nextEntries = entries.subList(entries.indexOf(entry), entries.size());
            seekingIterator.seek(entry.getKey());
            assertSequence(seekingIterator, nextEntries);

            seekingIterator.seek(before(entry));
            assertSequence(seekingIterator, nextEntries);

            seekingIterator.seek(after(entry));
            assertSequence(seekingIterator, nextEntries.subList(1, nextEntries.size()));
        }

        ChannelBuffer endKey = ChannelBuffers.wrappedBuffer(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
        seekingIterator.seek(endKey);
        assertSequence(seekingIterator, Collections.<BlockEntry>emptyList());
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

        FileUtils.deleteRecursively(databaseDir);
    }


    static Entry<ChannelBuffer, ChannelBuffer> createEntry(String key, String value)
    {
        return Maps.immutableEntry(toChannelBuffer(key), toChannelBuffer(value));
    }

    static ChannelBuffer toChannelBuffer(String value)
    {
        return ChannelBuffers.wrappedBuffer(value.getBytes(UTF_8));
    }

}
