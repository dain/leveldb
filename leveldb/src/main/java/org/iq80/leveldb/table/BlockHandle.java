package org.iq80.leveldb.table;

import org.iq80.leveldb.util.VariableLengthQuantity;
import org.jboss.netty.buffer.ChannelBuffer;

public class BlockHandle
{
    public static final int MAX_ENCODED_LENGTH = 10 + 10;

    private final long offset;
    private final int dataSize;

    BlockHandle(long offset, int dataSize)
    {
        this.offset = offset;
        this.dataSize = dataSize;
    }

    public long getOffset()
    {
        return offset;
    }

    public int getDataSize()
    {
        return dataSize;
    }

    public int getFullBlockSize() {
        return dataSize + BlockTrailer.ENCODED_LENGTH;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BlockHandle that = (BlockHandle) o;

        if (offset != that.offset) {
            return false;
        }
        if (dataSize != that.dataSize) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (offset ^ (offset >>> 32));
        result = 31 * result + (int) (dataSize ^ (dataSize >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("BlockHandle");
        sb.append("{offset=").append(offset);
        sb.append(", dataSize=").append(dataSize);
        sb.append('}');
        return sb.toString();
    }

    public static BlockHandle readBlockHandle(ChannelBuffer buffer)
    {
        long offset = VariableLengthQuantity.unpackLong(buffer);
        long size = VariableLengthQuantity.unpackLong(buffer);

        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Blocks can not be larger than Integer.MAX_VALUE");
        }

        return new BlockHandle(offset, (int) size);
    }

    public static void writeBlockHandle(BlockHandle blockHandle, ChannelBuffer buffer)
    {
        VariableLengthQuantity.packLong(blockHandle.offset, buffer);
        VariableLengthQuantity.packLong(blockHandle.dataSize, buffer);
    }
}
