/**
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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import org.jboss.netty.buffer.ChannelBuffer;
import org.iq80.leveldb.util.Buffers;
import sun.nio.ch.FileChannelImpl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.iq80.leveldb.impl.LogConstants.BLOCK_SIZE;
import static org.iq80.leveldb.impl.LogConstants.HEADER_SIZE;
import static org.iq80.leveldb.impl.Logs.getChunkChecksum;

public class MMapLogWriter implements LogWriter
{
    private static final Method unmap;
    private static final int PAGE_SIZE  = 1024 * 1024;

    static {
        Method x;
        try {
            x = FileChannelImpl.class.getDeclaredMethod("unmap", MappedByteBuffer.class);
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
        x.setAccessible(true);
        unmap = x;

    }

    private final File file;
    private final long fileNumber;
    private final FileChannel fileChannel;
    private final AtomicBoolean closed = new AtomicBoolean();
    private MappedByteBuffer mappedByteBuffer;
    private ChannelBuffer fileMap;
    private long fileOffset;
    /**
     * Current offset in the current block
     */
    private int blockOffset;

    public MMapLogWriter(File file, long fileNumber)
            throws IOException
    {
        Preconditions.checkNotNull(file, "file is null");
        Preconditions.checkArgument(fileNumber >= 0, "fileNumber is negative");

        this.file = file;
        this.fileNumber = fileNumber;
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, PAGE_SIZE);
        fileMap = Buffers.wrappedBuffer(mappedByteBuffer);
        fileMap.clear();
    }

    public boolean isClosed()
    {
        return closed.get();
    }

    public synchronized void close()
            throws IOException
    {
        closed.set(true);

        destroyMappedByteBuffer();

        if (fileChannel.isOpen()) {
            fileChannel.truncate(fileOffset);
        }

        // close the channel
        Closeables.closeQuietly(fileChannel);
    }

    public synchronized void delete()
            throws IOException
    {
        close();

        // try to delete the file
        file.delete();
    }

    private void destroyMappedByteBuffer()
    {
        if (mappedByteBuffer != null) {
            fileOffset += fileMap.readableBytes();
            unmap();
        }
        mappedByteBuffer = null;
        fileMap = null;
    }

    public File getFile()
    {
        return file;
    }

    public long getFileNumber()
    {
        return fileNumber;
    }

    // Writes a stream of chunks such that no chunk is split across a block boundary
    public synchronized void addRecord(ChannelBuffer record, boolean force)
            throws IOException
    {
        Preconditions.checkState(!closed.get(), "Log has been closed");

        record = record.slice();

        // used to track first, middle and last blocks
        boolean begin = true;

        // Fragment the record int chunks as necessary and write it.  Note that if record
        // is empty, we still want to iterate once to write a single
        // zero-length chunk.
        do {
            int bytesRemainingInBlock = BLOCK_SIZE - blockOffset;
            Preconditions.checkState(bytesRemainingInBlock >= 0);

            // Switch to a new block if necessary
            if (bytesRemainingInBlock < HEADER_SIZE) {
                if (bytesRemainingInBlock > 0) {
                    // Fill the rest of the block with zeros
                    // todo lame... need a better way to write zeros
                    ensureCapacity(bytesRemainingInBlock);
                    fileMap.writeZero(bytesRemainingInBlock);
                }
                blockOffset = 0;
                bytesRemainingInBlock = BLOCK_SIZE - blockOffset;
            }

            // Invariant: we never leave less than HEADER_SIZE bytes available in a block
            int bytesAvailableInBlock = bytesRemainingInBlock - HEADER_SIZE;
            Preconditions.checkState(bytesAvailableInBlock >= 0);

            // if there are more bytes in the record then there are available in the block,
            // fragment the record; otherwise write to the end of the record
            boolean end;
            int fragmentLength;
            if (record.readableBytes() >= bytesAvailableInBlock) {
                end = false;
                fragmentLength = bytesAvailableInBlock;
            }
            else {
                end = true;
                fragmentLength = record.readableBytes();
            }

            // determine block type
            LogChunkType type;
            if (begin && end) {
                type = LogChunkType.FULL;
            }
            else if (begin) {
                type = LogChunkType.FIRST;
            }
            else if (end) {
                type = LogChunkType.LAST;
            }
            else {
                type = LogChunkType.MIDDLE;
            }

            // write the chunk
            writeChunk(type, record, fragmentLength);

            // we are no longer on the first chunk
            begin = false;
        } while (record.readable());

        if (force) {
            mappedByteBuffer.force();
        }
    }

    private void writeChunk(LogChunkType type, ChannelBuffer buffer, int length)
            throws IOException
    {
        Preconditions.checkArgument(length <= 0xffff, "length %s is larger than two bytes", length);
        Preconditions.checkArgument(blockOffset + HEADER_SIZE + length <= BLOCK_SIZE);

        // create header
        ChannelBuffer header = newLogRecordHeader(type, buffer, length);

        // write the header and the payload
        ensureCapacity(header.readableBytes() + buffer.readableBytes());
        fileMap.writeBytes(header);
        fileMap.writeBytes(buffer, length);

        blockOffset += HEADER_SIZE + length;
    }

    private void ensureCapacity(int bytes)
            throws IOException
    {
        if (fileMap.writableBytes() < bytes) {
            // remap
            fileOffset += fileMap.readableBytes();
            unmap();

            mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, fileOffset, PAGE_SIZE);
            fileMap = Buffers.wrappedBuffer(mappedByteBuffer);
            fileMap.clear();
        }
    }

    private void unmap()
    {
        try {
            unmap.invoke(null, mappedByteBuffer);
        }
        catch (Exception ignored) {
            throw Throwables.propagate(ignored);
        }
    }

    private ChannelBuffer newLogRecordHeader(LogChunkType type, ChannelBuffer buffer, int length)
    {
        int crc = getChunkChecksum(type.getPersistentId(), buffer.array(), buffer.arrayOffset() + buffer.readerIndex(), length);

        // Format the header
        ChannelBuffer header = Buffers.buffer(HEADER_SIZE);
        header.writeInt(crc);
        header.writeByte((byte) (length & 0xff));
        header.writeByte((byte) (length >>> 8));
        header.writeByte((byte) (type.getPersistentId()));

        return header;
    }
}
