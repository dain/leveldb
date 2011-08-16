package org.iq80.leveldb.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closeables;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Charsets.UTF_8;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.FileAssert.fail;

public class LogTest
{
    private final LogMonitor NO_CORRUPTION_MONITOR = new LogMonitor()
    {
        @Override
        public void corruption(long bytes, String reason)
        {
            fail(String.format("corruption of %s bytes: %s", bytes, reason));
        }

        @Override
        public void corruption(long bytes, Throwable reason)
        {
            throw new RuntimeException(String.format("corruption of %s bytes: %s", bytes, reason), reason);
        }
    };

    private LogWriter writer;

    @Test
    public void testEmptyBlock()
            throws Exception
    {
        testLog();
    }

    @Test
    public void testSmallRecord()
            throws Exception
    {
        testLog(toChannelBuffer("dain sundstrom"));
    }


    @Test
    public void testMultipleSmallRecords()
            throws Exception
    {
        List<ChannelBuffer> records = Arrays.asList(
                toChannelBuffer("Lagunitas  Little Sumpin’ Sumpin’"),
                toChannelBuffer("Lagunitas IPA"),
                toChannelBuffer("Lagunitas Imperial Stout"),
                toChannelBuffer("Oban 14"),
                toChannelBuffer("Highland Park"),
                toChannelBuffer("Lagavulin"));

        testLog(records);
    }

    @Test
    public void testLargeRecord()
            throws Exception
    {
        testLog(toChannelBuffer("dain sundstrom", 4000));
    }

    @Test
    public void testMultipleLargeRecords()
            throws Exception
    {
        List<ChannelBuffer> records = Arrays.asList(
                toChannelBuffer("Lagunitas  Little Sumpin’ Sumpin’", 4000),
                toChannelBuffer("Lagunitas IPA", 4000),
                toChannelBuffer("Lagunitas Imperial Stout", 4000),
                toChannelBuffer("Oban 14", 4000),
                toChannelBuffer("Highland Park", 4000),
                toChannelBuffer("Lagavulin", 4000));

        testLog(records);
    }

    @Test
    public void testReadWithoutProperClose()
            throws Exception
    {
        testLog(ImmutableList.of(toChannelBuffer("something"), toChannelBuffer("something else")), false);
    }

    private void testLog(ChannelBuffer... entries)
            throws IOException
    {
        testLog(asList(entries));
    }

    private void testLog(List<ChannelBuffer> records)
            throws IOException
    {
        testLog(records, true);
    }

    private void testLog(List<ChannelBuffer> records, boolean closeWriter)
            throws IOException
    {
        for (ChannelBuffer entry : records) {
            writer.addRecord(entry, false);
        }

        if (closeWriter) {
            writer.close();
        }

        // test readRecord
        FileChannel fileChannel = new FileInputStream(writer.getFile()).getChannel();
        try {
            LogReader reader = new LogReader(fileChannel, NO_CORRUPTION_MONITOR, true, 0);
            for (ChannelBuffer expected : records) {
                ChannelBuffer actual = reader.readRecord();
                assertEquals(actual, expected);
            }
            assertNull(reader.readRecord());
        }
        finally {
            Closeables.closeQuietly(fileChannel);
        }
    }

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        writer = Logs.createLogWriter(File.createTempFile("table", ".log"), 42);
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        if (writer != null) {
            writer.delete();
        }
    }

    static ChannelBuffer toChannelBuffer(String value)
    {
        return toChannelBuffer(value, 1);
    }

    static ChannelBuffer toChannelBuffer(String value, int times)
    {
        byte[] bytes = value.getBytes(UTF_8);
        ChannelBuffer buffer = ChannelBuffers.buffer(bytes.length * times);
        for (int i = 0; i < times; i++) {
            buffer.writeBytes(bytes);
        }
        return buffer;
    }
}
