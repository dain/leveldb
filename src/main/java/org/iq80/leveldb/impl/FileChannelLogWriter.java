package org.iq80.leveldb.impl;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import org.iq80.leveldb.util.Buffers;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.iq80.leveldb.impl.LogConstants.BLOCK_SIZE;
import static org.iq80.leveldb.impl.LogConstants.HEADER_SIZE;

public class FileChannelLogWriter implements LogWriter
{
    private final File file;
    private final long fileNumber;
    private final FileChannel fileChannel;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Current offset in the current block
     */
    private int blockOffset;

    public FileChannelLogWriter(File file, long fileNumber)
            throws FileNotFoundException
    {
        Preconditions.checkNotNull(file, "file is null");
        Preconditions.checkArgument(fileNumber >= 0, "fileNumber is negative");

        this.file = file;
        this.fileNumber = fileNumber;
        this.fileChannel = new FileOutputStream(file).getChannel();
    }

    @Override
    public boolean isClosed()
    {
        return closed.get();
    }

    @Override
    public synchronized void close()
    {
        closed.set(true);

        // try to forces the log to disk
        try {
            fileChannel.force(true);
        }
        catch (IOException ignored) {
        }

        // close the channel
        Closeables.closeQuietly(fileChannel);
    }

    @Override
    public synchronized void delete()
    {
        closed.set(true);

        // close the channel
        Closeables.closeQuietly(fileChannel);

        // try to delete the file
        file.delete();
    }

    @Override
    public File getFile()
    {
        return file;
    }

    @Override
    public long getFileNumber()
    {
        return fileNumber;
    }

    // Writes a stream of chunks such that no chunk is split across a block boundary
    @Override
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
                    fileChannel.write(Buffers.byteBufferWrap(new byte[bytesRemainingInBlock]));
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
            fileChannel.force(false);
        }
    }

    private void writeChunk(LogChunkType type, ChannelBuffer buffer, int length)
            throws IOException
    {
        Preconditions.checkArgument(length <= 0xffff, "length %s is larger than two bytes", length);
        Preconditions.checkArgument(blockOffset + HEADER_SIZE + length <= BLOCK_SIZE);

        // create header
        ByteBuffer header = newLogRecordHeader(type, buffer, length);

        // write the header and the payload
        fileChannel.write(header);
        fileChannel.write(buffer.toByteBuffers(buffer.readerIndex(), length));
        buffer.skipBytes(length);

        blockOffset += HEADER_SIZE + length;
    }

    private ByteBuffer newLogRecordHeader(LogChunkType type, ChannelBuffer buffer, int length)
    {
        int crc = Logs.getChunkChecksum(type.getPersistentId(), buffer.array(), buffer.arrayOffset() + buffer.readerIndex(), length);

        // Format the header
        ByteBuffer header = Buffers.allocateByteBuffer(HEADER_SIZE);
        header.putInt(crc);
        header.put((byte) (length & 0xff));
        header.put((byte) (length >>> 8));
        header.put((byte) (type.getPersistentId()));

        header.flip();

        return header;
    }

}
