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

import org.iq80.leveldb.impl.LogListener.LogStructure;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.SliceInput;
import org.iq80.leveldb.util.SliceOutput;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.iq80.leveldb.impl.LogConstants.BLOCK_SIZE;
import static org.iq80.leveldb.impl.LogConstants.HEADER_SIZE;
import static org.junit.Assert.assertEquals;

/**
 * Tests for checking the reading and writing of log files.
 *
 * @author <a href="mailto:adam@evolvedbinary.com>Adam Retter</a>
 */
@RunWith(Parameterized.class)
public class LogIntegrationTest
{
    @ClassRule
    public static final TemporaryFolder tempFolder = new TemporaryFolder();

    @Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[][]{
                {"FileChannelLogWriter", FileChannelLogWriter.class},
                {"MMapLogWriter", MMapLogWriter.class}
        });
    }

    private static final int TOTAL_RECORDS = 10;
    private static final int MAX_RECORD_SIZE = BLOCK_SIZE - HEADER_SIZE;
    private static final LogMonitor EXCEPTION_ON_CORRUPTION_MONITOR = new ExceptionOnCorruptionMonitor();
    private static final Random random = new Random();

    @Parameter
    public String logWriterName;

    @Parameterized.Parameter(value = 1)
    public Class<LogWriter> logWriterClass;

    @Test
    public void writeLargeRecords_readForwards() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IOException
    {
        final int largeSize = (int) Math.round(MAX_RECORD_SIZE * 1.6);   // approx 1.6 times the record size
        final List<LogEvent> actualLogEvents = writeRecords_readForwards(largeSize);

        final List<LogEvent> expectedLogEvents = flatten(Arrays.asList(
            BLOCK(0, 32767,
                FIRST_CHUNK(0, 32767)
            ),
            BLOCK(32768, 65535,
                LAST_CHUNK(32768, 52431),
                FIRST_CHUNK(52432, 65535)
            ),
            BLOCK(65536, 98303,
                MIDDLE_CHUNK(65536, 98303)
            ),
            BLOCK(98304, 131071,
                LAST_CHUNK(98304, 104870),
                FIRST_CHUNK(104871, 131071)
            ),
            BLOCK(131072, 163839,
                LAST_CHUNK(131072, 157302),
                FIRST_CHUNK(157303, 163839)
            ),
            BLOCK(163840, 196607,
                MIDDLE_CHUNK(163840, 196607)
            ),
            BLOCK(196608, 229375,
                LAST_CHUNK(196608, 209741),
                FIRST_CHUNK(209742, 229375)
            ),
            BLOCK(229376, 262143,
                MIDDLE_CHUNK(229376, 262143)
            ),
            BLOCK(262144, 294911,
                LAST_CHUNK(262144, 262180),
                FIRST_CHUNK(262181, 294911)
            ),
            BLOCK(294912, 327679,
                LAST_CHUNK(294912, 314612),
                FIRST_CHUNK(314613, 327679)
            ),
            BLOCK(327680, 360447,
                MIDDLE_CHUNK(327680, 360447)
            ),
            BLOCK(360448, 393215,
                LAST_CHUNK(360448, 367051),
                FIRST_CHUNK(367052, 393215)
            ),
            BLOCK(393216, 425983,
                LAST_CHUNK(393216, 419483),
                FIRST_CHUNK(419484, 425983)
            ),
            BLOCK(425984, 458751,
                MIDDLE_CHUNK(425984, 458751)
            ),
            BLOCK(458752, 491519,
                LAST_CHUNK(458752, 471922),
                FIRST_CHUNK(471923, 491519)
            ),
            BLOCK(491520, 524287,
                MIDDLE_CHUNK(491520, 524287)
            ),
            BLOCK(524288, 524361,
                LAST_CHUNK(524288, 524361)
            )
        ));

        assertEquals(expectedLogEvents, actualLogEvents);
    }

    @Test
    public void writeFullRecords_readForwards() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IOException
    {
        final List<LogEvent> actualLogEvents = writeRecords_readForwards(MAX_RECORD_SIZE);

        final List<LogEvent> expectedLogEvents = flatten(Arrays.asList(
            BLOCK(0, 32767,
                FULL_CHUNK(0, 32767)
            ),
            BLOCK(32768, 65535,
                FULL_CHUNK(32768, 65535)
            ),
            BLOCK(65536, 98303,
                FULL_CHUNK(65536, 98303)
            ),
            BLOCK(98304, 131071,
                FULL_CHUNK(98304, 131071)
            ),
            BLOCK(131072, 163839,
                FULL_CHUNK(131072, 163839)
            ),
            BLOCK(163840, 196607,
                FULL_CHUNK(163840, 196607)
            ),
            BLOCK(196608, 229375,
                FULL_CHUNK(196608, 229375)
            ),
            BLOCK(229376, 262143,
                FULL_CHUNK(229376, 262143)
            ),
            BLOCK(262144, 294911,
                FULL_CHUNK(262144, 294911)
            ),
            BLOCK(294912, 327679,
                FULL_CHUNK(294912, 327679)
            )
        ));

        assertEquals(expectedLogEvents, actualLogEvents);
    }

    @Test
    public void writeHalfRecords_readForwards() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IOException
    {
        final int halfUp = Math.round((float) MAX_RECORD_SIZE / 2);
        final int halfDown = MAX_RECORD_SIZE / 2;
        if (halfUp != halfDown) {
            final List<LogEvent> actualLogEvents = new ArrayList<>();
            actualLogEvents.addAll(writeRecords_readForwards(halfUp));
            actualLogEvents.addAll(writeRecords_readForwards(halfDown));

            final List<LogEvent> expectedLogEvents = flatten(Arrays.asList(
                BLOCK(0, 32767,
                    FULL_CHUNK(0, 16387),
                    FIRST_CHUNK(16388, 32767)
                ),
                BLOCK(32768, 65535,
                    LAST_CHUNK(32768, 32782),
                    FULL_CHUNK(32783, 49170),
                    FIRST_CHUNK(49171, 65535)
                ),
                BLOCK(65536, 98303,
                    LAST_CHUNK(65536, 65565),
                    FULL_CHUNK(65566, 81953),
                    FIRST_CHUNK(81954, 98303)
                ),
                BLOCK(98304, 131071,
                    LAST_CHUNK(98304, 98348),
                    FULL_CHUNK(98349, 114736),
                    FIRST_CHUNK(114737, 131071)
                ),
                BLOCK(131072, 163839,
                    LAST_CHUNK(131072, 131131),
                    FULL_CHUNK(131132, 147519),
                    FIRST_CHUNK(147520, 163839)
                ),
                BLOCK(163840, 163914,
                    LAST_CHUNK(163840, 163914)
                ),
                BLOCK(0, 32767,
                    FULL_CHUNK(0, 16386),
                    FIRST_CHUNK(16387, 32767)
                ),
                BLOCK(32768, 65535,
                    LAST_CHUNK(32768, 32780),
                    FULL_CHUNK(32781, 49167),
                    FIRST_CHUNK(49168, 65535)
                ),
                BLOCK(65536, 98303,
                    LAST_CHUNK(65536, 65561),
                    FULL_CHUNK(65562, 81948),
                    FIRST_CHUNK(81949, 98303)
                ),
                BLOCK(98304, 131071,
                    LAST_CHUNK(98304, 98342),
                    FULL_CHUNK(98343, 114729),
                    FIRST_CHUNK(114730, 131071)
                ),
                BLOCK(131072, 163839,
                    LAST_CHUNK(131072, 131123),
                    FULL_CHUNK(131124, 147510),
                    FIRST_CHUNK(147511, 163839)
                ),
                BLOCK(163840, 163904,
                    LAST_CHUNK(163840, 163904)
                )
            ));

            assertEquals(expectedLogEvents, actualLogEvents);

        }
        else {
            final List<LogEvent> actualLogEventsHalfUp = writeRecords_readForwards(halfUp);
        }
    }

    @Test
    public void writeSmallRecords_readForwards() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IOException
    {
        final int smallSize = Math.min(Math.round((float) MAX_RECORD_SIZE / 4), 4 * 1024);   // A quarter the max record size or 4 KB, whichever is smaller
        final List<LogEvent> actualLogEvents = writeRecords_readForwards(smallSize);

        final List<LogEvent> expectedLogEvents = flatten(Arrays.asList(
            BLOCK(0, 32767,
                FULL_CHUNK(0, 4102),
                FULL_CHUNK(4103, 8205),
                FULL_CHUNK(8206, 12308),
                FULL_CHUNK(12309, 16411),
                FULL_CHUNK(16412, 20514),
                FULL_CHUNK(20515, 24617),
                FULL_CHUNK(24618, 28720),
                FIRST_CHUNK(28721, 32767)           // len = 4040
            ),
            BLOCK(32768, 41036,
                LAST_CHUNK(32768, 32830),           // len = 56
                FULL_CHUNK(32831, 36933),
                FULL_CHUNK(36934, 41036)
            )
        ));

        assertEquals(expectedLogEvents, actualLogEvents);
    }

    @Test
    public void readSecondRecord_forwards() throws IOException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException
    {
        final long initialOffset = MAX_RECORD_SIZE;    // MAX_RECORD_SIZE positions us at the start of record 2
        final List<LogEvent> actualLogEvents = writeRecords_readForwards(MAX_RECORD_SIZE, initialOffset);

        final List<LogEvent> expectedLogEvents = flatten(Arrays.asList(
            BLOCK(0, 32767,
                BAD_CHUNK(0, 6)
            ),
            BLOCK(32768, 65535,
                FULL_CHUNK(32768, 65535)
            ),
            BLOCK(65536, 98303,
                FULL_CHUNK(65536, 98303)
            ),
            BLOCK(98304, 131071,
                FULL_CHUNK(98304, 131071)
            ),
            BLOCK(131072, 163839,
                FULL_CHUNK(131072, 163839)
            ),
            BLOCK(163840, 196607,
                FULL_CHUNK(163840, 196607)
            ),
            BLOCK(196608, 229375,
                FULL_CHUNK(196608, 229375)
            ),
            BLOCK(229376, 262143,
                FULL_CHUNK(229376, 262143)
            ),
            BLOCK(262144, 294911,
                FULL_CHUNK(262144, 294911)
            ),
            BLOCK(294912, 327679,
                FULL_CHUNK(294912, 327679)
            )
        ));

        assertEquals(expectedLogEvents, actualLogEvents);
    }

    @Test
    public void readFifthRecord_forward() throws IOException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException
    {
        final long initialOffset = (long) (MAX_RECORD_SIZE * 4) + 10;    // (MAX_RECORD_SIZE * 4) positions us at the start of record 4, the + 10 moves us into record 4, so we will skip to next physical record i.e. record 5
        final List<LogEvent> actualLogEvents = writeRecords_readForwards(MAX_RECORD_SIZE, initialOffset);

        final List<LogEvent> expectedLogEvents = flatten(Arrays.asList(
            BLOCK(98304, 131071,
                BAD_CHUNK(98304, 98310)
            ),
            BLOCK(131072, 163839,
                FULL_CHUNK(131072, 163839)
            ),
            BLOCK(163840, 196607,
                FULL_CHUNK(163840, 196607)
            ),
            BLOCK(196608, 229375,
                FULL_CHUNK(196608, 229375)
            ),
            BLOCK(229376, 262143,
                FULL_CHUNK(229376, 262143)
            ),
            BLOCK(262144, 294911,
                FULL_CHUNK(262144, 294911)
            ),
            BLOCK(294912, 327679,
                FULL_CHUNK(294912, 327679)
            )
        ));

        assertEquals(expectedLogEvents, actualLogEvents);
    }

    @Test
    public void readLastRecord_forward() throws IOException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException
    {
        final long fileSize = BLOCK_SIZE * TOTAL_RECORDS;
        final int numCompleteBlocks = (int) fileSize / BLOCK_SIZE;
        final long initialOffset = BLOCK_SIZE * (numCompleteBlocks - 1);    // BLOCK_SIZE * (numCompleteBlocks - 1) positions us at the start of record 10
        final List<LogEvent> actualLogEvents = writeRecords_readForwards(MAX_RECORD_SIZE, initialOffset);

        final List<LogEvent> expectedLogEvents = Arrays.asList(
            BLOCK(294912, 327679,
                FULL_CHUNK(294912, 327679)
            )
        );

        assertEquals(expectedLogEvents, actualLogEvents);
    }

    private List<LogEvent> writeRecords_readForwards(final int recordSize) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException
    {
        return writeRecords_readForwards(recordSize, 0);
    }

    private List<LogEvent> writeRecords_readForwards(final int recordSize, final long initialOffset) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException
    {
        final Path logFile = tempFolder.newFile().toPath();

        // write the log file
        final String[] expectedStrings = writeRecords(logFile, recordSize);

        // read back the log file
        try (final FileInputStream fis = new FileInputStream(logFile.toFile());
            final FileChannel logFileChannel = fis.getChannel()) {
            final LogReader logReader = new LogReader(logFileChannel, EXCEPTION_ON_CORRUPTION_MONITOR, true, initialOffset);

            final CollectingLogListener collectingLogListener = new CollectingLogListener();
            logReader.addListener(collectingLogListener);

            Slice readBuf = null;
            //int recordIdx = 0;
            int recordIdx = (int) initialOffset / MAX_RECORD_SIZE;
            while ((readBuf = logReader.readRecord()) != null) {
                assertEquals(recordSize, readBuf.length());

                final String actualString = getString(readBuf.input());
                assertEquals(expectedStrings[recordIdx], actualString);

                recordIdx++;
            }

            assertEquals(TOTAL_RECORDS, recordIdx);

            return collectingLogListener.logEvents;
        }
    }

    private String[] writeRecords(final Path logFile, final int recordSize) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IOException
    {
        final SliceOutput writeBuf = new Slice(recordSize).output();

        final String[] expectedStrings = new String[TOTAL_RECORDS];

        // write a log file
        final LogWriter logWriter = createLogWriter(logFile, 1);
        try {
            for (int i = 0; i < TOTAL_RECORDS; i++) {
                expectedStrings[i] = "record" + i;
                putString(writeBuf, expectedStrings[i]);
                fillRandom(writeBuf);

                logWriter.addRecord(writeBuf.slice(), false);

                writeBuf.reset();
            }
        }
        finally {
            logWriter.close();
        }

        return expectedStrings;
    }

    private LogWriter createLogWriter(final Path file, final long fileNumber) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException
    {
        final Constructor<LogWriter> constructor = logWriterClass.getConstructor(File.class, long.class);
        return constructor.newInstance(file.toFile(), fileNumber);
    }

    private static void putString(final SliceOutput buf, final String string) throws IOException
    {
        final byte[] data = string.getBytes(UTF_8);
        buf.writeInt(data.length);
        buf.writeBytes(data);
    }

    private static String getString(final SliceInput buf)
    {
        final int dataLen = buf.readInt();
        final byte[] data = buf.readBytes(dataLen).getBytes();
        return new String(data, UTF_8);
    }

    private static void fillRandom(final SliceOutput buf)
    {
        final int remaining = buf.writableBytes();
        final byte[] data = new byte[remaining];
        random.nextBytes(data);
        buf.writeBytes(data);
    }

    private static class ExceptionOnCorruptionMonitor implements LogMonitor
    {
        @Override
        public void corruption(final long bytes, final String reason)
        {
            throw new RuntimeException(bytes + " bytes corrupted: " + reason);
        }

        @Override
        public void corruption(final long bytes, final Throwable reason)
        {
            throw new RuntimeException(bytes + " bytes corrupted: " + reason.getMessage(), reason);
        }
    }

    private static class CollectingLogListener implements LogListener
    {
        final List<LogEvent> logEvents = new ArrayList<>();

        @Override
        public void event(final LogStructure logStructure, final long startOffset, final long endOffset)
        {
            logEvents.add(new LogEvent(logStructure, startOffset, endOffset));
        }
    }

    /**
     * Useful for debugging log events!
     */
    private static void printLogEvents(final List<LogEvent> logEvents)
    {
        boolean firstBlock = true;

        for (int i = 0; i < logEvents.size(); i++) {
            final LogEvent logEvent = logEvents.get(i);

            String str = String.format("%s(%d, %s", logEvent.logStructure.name(), logEvent.startOffset, logEvent.endOffset);
            if (logEvent.logStructure == LogStructure.BLOCK) {
                if (!firstBlock) {
                    System.out.println("),");
                }
                firstBlock = false;

                str += ",";
            }
            else {
                str = "    " + str + ")";
                if (i < logEvents.size() - 1) {
                    if (logEvents.get(i + 1).logStructure != LogStructure.BLOCK) {
                        str += ",";
                    }
                }
            }

            System.out.println(str);

            //close last block
            if (i == logEvents.size() - 1) {
                System.out.println(")");
            }
        }
    }

    private static List<LogEvent> flatten(final List<LogEvent[]> logEvents)
    {
        final List<LogEvent> results = new ArrayList<>();
        for (final LogEvent[] les : logEvents) {
            for (final LogEvent le : les) {
                results.add(le);
            }
        }
        return results;
    }

    private static LogEvent[] BLOCK(final long startOffset, final long endOffset, final LogEvent... chunkEvents)
    {
        final LogEvent[] logEvents = new LogEvent[chunkEvents.length + 1];
        int i = 0;
        logEvents[i++] = new LogEvent(LogStructure.BLOCK, startOffset, endOffset);
        for (final LogEvent chunkEvent : chunkEvents) {
            if (chunkEvent.logStructure == LogStructure.BLOCK) {
                throw new IllegalStateException("Blocks cannot be nested");
            }

            //first chunk start must match block start
            if(i == 1 && chunkEvent.startOffset != startOffset) {
                throw new IllegalStateException(String.format("First chunk startOffset: %d within block must start at block startOffset: %d", chunkEvent.startOffset, startOffset));
            } else if (chunkEvent.startOffset < startOffset) {
                throw new IllegalStateException(String.format("Chunk startOffset: %d cannot be less than block startOffset: %d", chunkEvent.startOffset, startOffset));
            }

            //last chunk end must match block end
            if(i == chunkEvents.length && chunkEvent.endOffset != endOffset && chunkEvent.logStructure != LogStructure.BAD_CHUNK) {
                throw new IllegalStateException(String.format("Last chunk endOffset: %d within block must end at block endOffset: %d", chunkEvent.endOffset, endOffset));
            } else if (chunkEvent.endOffset > endOffset) {
                throw new IllegalStateException(String.format("Chunk endOffset: %d cannot be less than block endOffset: %d", chunkEvent.startOffset, startOffset));
            }

            logEvents[i++] = chunkEvent;
        }
        return logEvents;
    }

    private static LogEvent FULL_CHUNK(final long startOffset, final long endOffset)
    {
        return new LogEvent(LogStructure.FULL_CHUNK, startOffset, endOffset);
    }

    private static LogEvent FIRST_CHUNK(final long startOffset, final long endOffset)
    {
        return new LogEvent(LogStructure.FIRST_CHUNK, startOffset, endOffset);
    }

    private static LogEvent MIDDLE_CHUNK(final long startOffset, final long endOffset)
    {
        return new LogEvent(LogStructure.MIDDLE_CHUNK, startOffset, endOffset);
    }

    private static LogEvent LAST_CHUNK(final long startOffset, final long endOffset)
    {
        return new LogEvent(LogStructure.LAST_CHUNK, startOffset, endOffset);
    }

    private static LogEvent BAD_CHUNK(final long startOffset, final long endOffset)
    {
        return new LogEvent(LogStructure.BAD_CHUNK, startOffset, endOffset);
    }

    private static class LogEvent
    {
        final LogStructure logStructure;
        final long startOffset;
        final long endOffset;

        public LogEvent(final LogStructure logStructure, final long startOffset, final long endOffset)
        {
            this.logStructure = logStructure;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        @Override
        public boolean equals(final Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            final LogEvent otherLogEvent = (LogEvent) obj;

            if (startOffset != otherLogEvent.startOffset) {
                return false;
            }
            if (endOffset != otherLogEvent.endOffset) {
                return false;
            }
            return logStructure == otherLogEvent.logStructure;
        }

        @Override
        public int hashCode()
        {
            int result = logStructure.hashCode();
            result = 31 * result + (int) (startOffset ^ (startOffset >>> 32));
            result = 31 * result + (int) (endOffset ^ (endOffset >>> 32));
            return result;
        }

        @Override
        public String toString()
        {
            return String.format("%s [start=%d, end=%d]", logStructure.name(), startOffset, endOffset);
        }
    }
}
