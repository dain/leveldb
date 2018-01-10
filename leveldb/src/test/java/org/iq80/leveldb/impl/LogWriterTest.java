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

import org.iq80.leveldb.util.SequentialFile;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.SliceOutput;
import org.iq80.leveldb.util.WritableFile;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class LogWriterTest
{
    @Test
    public void testLogRecordBounds()
            throws Exception
    {
        StringSink open = new StringSink();

        int recordSize = LogConstants.BLOCK_SIZE - LogConstants.HEADER_SIZE;
        Slice record = new Slice(recordSize);

        LogWriter writer = LogWriter.createWriter(10, open);
        writer.addRecord(record, false);
        writer.close();

        LogMonitor logMonitor = new AssertNoCorruptionLogMonitor();

        try (SequentialFile in = new SequentialBytes(new ByteArrayInputStream(open.sb.toByteArray()))) {
            LogReader logReader = new LogReader(in, logMonitor, true, 0);
            int count = 0;
            for (Slice slice = logReader.readRecord(); slice != null; slice = logReader.readRecord()) {
                assertEquals(slice.length(), recordSize);
                count++;
            }
            assertEquals(count, 1);
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
        public void force()
        {
            content = sb.toByteArray();
        }

        @Override
        public void close() throws IOException
        {
            content = sb.toByteArray();
            sb.close();
        }
    }

    private static class SequentialBytes implements SequentialFile
    {
        private ByteArrayInputStream in;

        public SequentialBytes(ByteArrayInputStream in)
        {
            this.in = in;
        }

        @Override
        public void skip(long n)
        {
           assertEquals(in.skip(n), n);
        }

        @Override
        public int read(int atMost, SliceOutput destination) throws IOException
        {
            return destination.writeBytes(in, atMost);
        }

        @Override
        public void close() throws IOException
        {
            in.close();
        }
    }

    private static class AssertNoCorruptionLogMonitor
            implements LogMonitor
    {
        @Override
        public void corruption(long bytes, String reason)
        {
            fail("corruption at " + bytes + " reason: " + reason);
        }

        @Override
        public void corruption(long bytes, Throwable reason)
        {
            fail("corruption at " + bytes + " reason: " + reason);
        }
    }
}
