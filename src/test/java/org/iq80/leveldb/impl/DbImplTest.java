package org.iq80.leveldb.impl;

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
import static org.iq80.leveldb.table.BlockHelper.assertEntryEquals;
import static org.iq80.leveldb.table.BlockHelper.assertSequence;
import static org.iq80.leveldb.table.BlockHelper.before;
import static org.testng.Assert.assertEquals;

public class DbImplTest
{
    private File databaseDir;
    private DbImpl db;

    @Test
    public void testEmptyBlock()
            throws Exception
    {
        db = new DbImpl(new Options(), databaseDir);

        tableDb();
    }


    @Test
    public void testSingleEntrySingleBlock()
            throws Exception
    {
        db = new DbImpl(new Options(), databaseDir);
        tableDb(createEntry("name", "dain sundstrom"));
    }

    @Test
    public void testMultipleEntriesWithSingleBlock()
            throws Exception
    {
        db = new DbImpl(new Options().setWriteBufferSize(100), databaseDir);

        List<Entry<ChannelBuffer, ChannelBuffer>> entries = Arrays.asList(
                createEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                createEntry("beer/ipa", "Lagunitas IPA"),
                createEntry("beer/stout", "Lagunitas Imperial Stout"),
                createEntry("scotch/light", "Oban 14"),
                createEntry("scotch/medium", "Highland Park"),
                createEntry("scotch/strong", "Lagavulin"));

        for (int i = 1; i < entries.size(); i++) {
            tableDb(entries);
        }
    }

    @Test
    public void testMultipleEntriesWithMultipleBlock()
            throws Exception
    {
        db = new DbImpl(new Options(), databaseDir);

        List<Entry<ChannelBuffer, ChannelBuffer>> entries = Arrays.asList(
                createEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                createEntry("beer/ipa", "Lagunitas IPA"),
                createEntry("beer/stout", "Lagunitas Imperial Stout"),
                createEntry("scotch/light", "Oban 14"),
                createEntry("scotch/medium", "Highland Park"),
                createEntry("scotch/strong", "Lagavulin"));

        tableDb(entries);
    }

    private void tableDb(Entry<ChannelBuffer, ChannelBuffer>... entries)
            throws IOException
    {
        tableDb(asList(entries));
    }

    private void tableDb(List<Entry<ChannelBuffer, ChannelBuffer>> entries)
            throws IOException
    {

        for (Entry<ChannelBuffer, ChannelBuffer> entry : entries) {
            db.put(entry.getKey(), entry.getValue());
        }

        for (Entry<ChannelBuffer, ChannelBuffer> entry : entries) {
            assertEquals(db.get(entry.getKey()), entry.getValue());
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
