package org.iq80.leveldb.impl;

import org.jboss.netty.buffer.ChannelBuffer;
import org.iq80.leveldb.util.Buffers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.iq80.leveldb.impl.LogChunkType.BAD_CHUNK;
import static org.iq80.leveldb.impl.LogChunkType.EOF;
import static org.iq80.leveldb.impl.LogChunkType.UNKNOWN;
import static org.iq80.leveldb.impl.LogChunkType.ZERO_TYPE;
import static org.iq80.leveldb.impl.LogChunkType.getLogChunkTypeByPersistentId;
import static org.iq80.leveldb.impl.LogConstants.BLOCK_SIZE;
import static org.iq80.leveldb.impl.LogConstants.HEADER_SIZE;
import static org.iq80.leveldb.impl.Logs.getChunkChecksum;

public class LogReader
{
    private final FileChannel fileChannel;

    private final LogMonitor monitor;

    private final boolean verifyChecksums;

    /**
     * Offset at which to start looking for the first record to return
     */
    private final long initialOffset;

    /**
     * Have we read to the end of the file?
     */
    private boolean eof;

    /**
     * Offset of the last record returned by readRecord.
     */
    private long lastRecordOffset;

    /**
     * Offset of the first location past the end of buffer.
     */
    private long endOfBufferOffset;

    private ChannelBuffer scratch = Buffers.dynamicBuffer(BLOCK_SIZE);

    private ChannelBuffer currentChunk;

    private ByteBuffer currentBlock = Buffers.allocateByteBuffer(BLOCK_SIZE);

    public LogReader(FileChannel fileChannel, LogMonitor monitor, boolean verifyChecksums, long initialOffset)
    {
        this.fileChannel = fileChannel;
        this.monitor = monitor;
        this.verifyChecksums = verifyChecksums;
        this.initialOffset = initialOffset;
        currentBlock.limit(0);
    }

    public long getLastRecordOffset()
    {
        return lastRecordOffset;
    }

    /**
     * Skips all blocks that are completely before "initial_offset_".
     * <p/>
     * Handles reporting corruption
     *
     * @return true on success.
     */
    private boolean skipToInitialBlock()
    {
        int offsetInBlock = (int) (initialOffset % BLOCK_SIZE);
        long blockStartLocation = initialOffset - offsetInBlock;

        // Don't search a block if we'd be in the trailer
        if (offsetInBlock > BLOCK_SIZE - 6) {
            blockStartLocation += BLOCK_SIZE;
        }

        endOfBufferOffset = blockStartLocation;

        // Skip to start of first block that can contain the initial record
        if (blockStartLocation > 0) {
            try {
                fileChannel.position(blockStartLocation);
            }
            catch (IOException e) {
                reportDrop(blockStartLocation, e);
                return false;
            }
        }

        return true;
    }

    public ChannelBuffer readRecord()
    {
        scratch.clear();

        // advance to the first record, if we haven't already
        if (lastRecordOffset < initialOffset) {
            if (!skipToInitialBlock()) {
                return null;
            }
        }

        // Record offset of the logical record that we're reading
        long prospectiveRecordOffset = 0;

        boolean inFragmentedRecord = false;
        while (true) {
            long physicalRecordOffset = endOfBufferOffset - readableBytes(currentBlock);
            LogChunkType chunkType = readNextChunk();
            switch (chunkType) {
                case FULL:
                    if (inFragmentedRecord) {
                        reportCorruption(scratch.readableBytes(), "Partial record without end");
                        // simply return this full block
                    }
                    scratch.clear();
                    prospectiveRecordOffset = physicalRecordOffset;
                    lastRecordOffset = prospectiveRecordOffset;
                    return currentChunk;

                case FIRST:
                    if (inFragmentedRecord) {
                        reportCorruption(scratch.readableBytes(), "Partial record without end");
                        // clear the scratch and start over from this chunk
                        scratch.clear();
                    }
                    prospectiveRecordOffset = physicalRecordOffset;
                    scratch.writeBytes(currentChunk);
                    inFragmentedRecord = true;
                    break;

                case MIDDLE:
                    if (!inFragmentedRecord) {
                        reportCorruption(scratch.readableBytes(), "Missing start of fragmented record");

                        // clear the scratch and skip this chunk
                        scratch.clear();
                    }
                    else {
                        scratch.writeBytes(currentChunk);
                    }
                    break;

                case LAST:
                    if (!inFragmentedRecord) {
                        reportCorruption(scratch.readableBytes(), "Missing start of fragmented record");

                        // clear the scratch and skip this chunk
                        scratch.clear();
                    }
                    else {
                        scratch.writeBytes(currentChunk);
                        lastRecordOffset = prospectiveRecordOffset;
                        return scratch;
                    }
                    break;

                case EOF:
                    if (inFragmentedRecord) {
                        reportCorruption(scratch.readableBytes(), "Partial record without end");

                        // clear the scratch and return
                        scratch.clear();
                    }
                    return null;

                case BAD_CHUNK:
                    if (inFragmentedRecord) {
                        reportCorruption(scratch.readableBytes(), "Error in middle of record");
                        inFragmentedRecord = false;
                        scratch.clear();
                    }
                    break;

                default:
                    int dropSize = currentChunk.readableBytes();
                    if (inFragmentedRecord) {
                        dropSize += scratch.readableBytes();
                    }
                    reportCorruption(dropSize, String.format("Unexpected chunk type %s", chunkType));
                    inFragmentedRecord = false;
                    scratch.clear();
                    break;

            }
        }
    }

    /**
     * Return type, or one of the preceding special values
     */
    private LogChunkType readNextChunk()
    {
        // clear the current chunk
        currentChunk = null;

        // read the next block if necessary
        if (readableBytes(currentBlock) < HEADER_SIZE) {
            if (!readNextBlock()) {
                if (eof) {
                    return EOF;
                }
            }
        }

        // parse header
        int expectedChecksum = currentBlock.getInt();
        int length = (currentBlock.get() & 0xff);
        length = length | (currentBlock.get() & 0xff) << 8;
        byte chunkTypeId = currentBlock.get();
        LogChunkType chunkType = getLogChunkTypeByPersistentId(chunkTypeId);

        // verify length
        if (length > readableBytes(currentBlock)) {
            int dropSize = readableBytes(currentBlock) + HEADER_SIZE;
            currentBlock.clear();
            currentBlock.limit(0);
            reportCorruption(dropSize, "Invalid chunk length");
            return BAD_CHUNK;
        }

        // skip zero length records
        if (chunkType == ZERO_TYPE && length == 0) {
            // Skip zero length record without reporting any drops since
            // such records are produced by the writing code.
            currentBlock.clear();
            currentBlock.limit(0);
            return BAD_CHUNK;
        }

        if (verifyChecksums) {
            int actualChecksum = getChunkChecksum(chunkTypeId, currentBlock.array(), currentBlock.arrayOffset() + currentBlock.position(), length);
            if (actualChecksum != expectedChecksum) {
                // Drop the rest of the buffer since "length" itself may have
                // been corrupted and if we trust it, we could find some
                // fragment of a real log record that just happens to look
                // like a valid log record.
                int dropSize = readableBytes(currentBlock) + HEADER_SIZE;
                currentBlock.clear();
                currentBlock.limit(0);
                reportCorruption(dropSize, "Invalid chunk checksum");
                return BAD_CHUNK;
            }
        }

        // Skip physical record that started before initialOffset
        if (endOfBufferOffset - HEADER_SIZE - length < initialOffset) {
            return BAD_CHUNK;
        }

        // Skip unknown chunk types
        // Since this comes last so we the, know it is a valid chunk, and is just a type we don't understand
        if (chunkType == UNKNOWN) {
            reportCorruption(length, String.format("Unknown chunk type %d", chunkType.getPersistentId()));
            return BAD_CHUNK;
        }

        // set chunk ot valid slice of the block buffer
        ByteBuffer slice = currentBlock.duplicate();
        // wow duplicate does not copy endianness!
        slice.order(LITTLE_ENDIAN);
        slice.limit(slice.position() + length);
        currentChunk = Buffers.wrappedBuffer(slice);

        // advance the blockBuffer to the end of this chunk
        currentBlock.position(currentBlock.position() + length);

        return chunkType;
    }

    private int readableBytes(ByteBuffer blockBuffer)
    {
        return blockBuffer.limit() - blockBuffer.position();
    }

    public boolean readNextBlock()
    {
        currentBlock.clear();

        if (eof) {
            return false;
        }

        while (currentBlock.remaining() > 0) {
            try {
                int bytesRead = fileChannel.read(currentBlock);
                if (bytesRead < 0) {
                    // no more bytes to read
                    eof = true;
                    break;
                }
                endOfBufferOffset += bytesRead;
            }
            catch (IOException e) {
                currentBlock.clear();
                currentBlock.limit(0);
                reportDrop(BLOCK_SIZE, e);
                eof = true;
                return false;
            }

        }
        currentBlock.flip();
        return currentBlock.limit() > 0;
    }

    /**
     * Reports corruption to the monitor.
     * The buffer must be updated to remove the dropped bytes prior to invocation.
     */
    private void reportCorruption(long bytes, String reason)
    {
        if (monitor != null) {
            monitor.corruption(bytes, reason);
        }
    }

    /**
     * Reports dropped bytes to the monitor.
     * The buffer must be updated to remove the dropped bytes prior to invocation.
     */
    private void reportDrop(long bytes, Throwable reason)
    {
        if (monitor != null) {
            monitor.corruption(bytes, reason);
        }
    }
}
