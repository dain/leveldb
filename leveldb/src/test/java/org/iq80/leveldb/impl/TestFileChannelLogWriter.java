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

import org.iq80.leveldb.util.Slice;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.nio.channels.FileChannel;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestFileChannelLogWriter
{
    @Test
    public void testLogRecordBounds()
            throws Exception
    {
        File file = File.createTempFile("test", ".log");
        try {
            int recordSize = LogConstants.BLOCK_SIZE - LogConstants.HEADER_SIZE;
            Slice record = new Slice(recordSize);

            LogWriter writer = new FileChannelLogWriter(file, 10);
            writer.addRecord(record, false);
            writer.close();

            LogMonitor logMonitor = new AssertNoCorruptionLogMonitor();

            FileChannel channel = new FileInputStream(file).getChannel();

            LogReader logReader = new LogReader(channel, logMonitor, true, 0);

            int count = 0;
            for (Slice slice = logReader.readRecord(); slice != null; slice = logReader.readRecord()) {
                assertEquals(slice.length(), recordSize);
                count++;
            }
            assertEquals(count, 1);
        }
        finally {
            file.delete();
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
