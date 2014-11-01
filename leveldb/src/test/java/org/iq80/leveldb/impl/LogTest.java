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

import com.google.common.collect.ImmutableList;
import org.iq80.leveldb.util.Closeables;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.SliceOutput;
import org.iq80.leveldb.util.Slices;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.List;

import static com.google.common.base.Charsets.UTF_8;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.FileAssert.fail;

public class LogTest
{
    private static final LogMonitor NO_CORRUPTION_MONITOR = new LogMonitor()
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
        testLog(toSlice("dain sundstrom"));
    }

    @Test
    public void testMultipleSmallRecords()
            throws Exception
    {
        List<Slice> records = asList(
                toSlice("Lagunitas  Little Sumpin’ Sumpin’"),
                toSlice("Lagunitas IPA"),
                toSlice("Lagunitas Imperial Stout"),
                toSlice("Oban 14"),
                toSlice("Highland Park"),
                toSlice("Lagavulin"));

        testLog(records);
    }

    @Test
    public void testLargeRecord()
            throws Exception
    {
        testLog(toSlice("dain sundstrom", 4000));
    }

    @Test
    public void testMultipleLargeRecords()
            throws Exception
    {
        List<Slice> records = asList(
                toSlice("Lagunitas  Little Sumpin’ Sumpin’", 4000),
                toSlice("Lagunitas IPA", 4000),
                toSlice("Lagunitas Imperial Stout", 4000),
                toSlice("Oban 14", 4000),
                toSlice("Highland Park", 4000),
                toSlice("Lagavulin", 4000));

        testLog(records);
    }

    @Test
    public void testReadWithoutProperClose()
            throws Exception
    {
        testLog(ImmutableList.of(toSlice("something"), toSlice("something else")), false);
    }

    private void testLog(Slice... entries)
            throws IOException
    {
        testLog(asList(entries));
    }

    private void testLog(List<Slice> records)
            throws IOException
    {
        testLog(records, true);
    }

    private void testLog(List<Slice> records, boolean closeWriter)
            throws IOException
    {
        for (Slice entry : records) {
            writer.addRecord(entry, false);
        }

        if (closeWriter) {
            writer.close();
        }

        // test readRecord
        FileChannel fileChannel = new FileInputStream(writer.getFile()).getChannel();
        try {
            LogReader reader = new LogReader(fileChannel, NO_CORRUPTION_MONITOR, true, 0);
            for (Slice expected : records) {
                Slice actual = reader.readRecord();
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

    static Slice toSlice(String value)
    {
        return toSlice(value, 1);
    }

    static Slice toSlice(String value, int times)
    {
        byte[] bytes = value.getBytes(UTF_8);
        Slice slice = Slices.allocate(bytes.length * times);
        SliceOutput sliceOutput = slice.output();
        for (int i = 0; i < times; i++) {
            sliceOutput.writeBytes(bytes);
        }
        return slice;
    }
}
