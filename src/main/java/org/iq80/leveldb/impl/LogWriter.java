package org.iq80.leveldb.impl;

import com.google.common.base.Preconditions;
import org.iq80.leveldb.util.PureJavaCrc32C;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.iq80.leveldb.impl.LogConstants.BLOCK_SIZE;
import static org.iq80.leveldb.impl.LogConstants.HEADER_SIZE;

public class LogWriter
{
    private final FileChannel fileChannel;

    /**
     * Current offset in the current block
     */
    private int blockOffset;

    public LogWriter(FileChannel fileChannel)
    {
        this.fileChannel = fileChannel;
    }

    // Writes a stream of chunks such that no chunk is split across a block boundary

    public void addRecord(ChannelBuffer record)
            throws IOException
    {
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
                    fileChannel.write(ByteBuffer.wrap(new byte[bytesRemainingInBlock]));
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
        int crc = getChunkChecksum(type.getPersistentId(), buffer.array(), buffer.arrayOffset() + buffer.readerIndex(), length);

        // Format the header
        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
        header.putInt(crc);
        header.put((byte) (length & 0xff));
        header.put((byte) (length >>> 8));
        header.put((byte) (type.getPersistentId()));

        header.flip();

        return header;
    }

    public static int getChunkChecksum(int chunkTypeId, byte[] buffer, int offset, int length)
    {
        // Compute the crc of the record type and the payload.
        PureJavaCrc32C crc32C = new PureJavaCrc32C();
        crc32C.update(chunkTypeId);
        crc32C.update(buffer, offset, length);
        return crc32C.getMaskedValue();
    }
}
