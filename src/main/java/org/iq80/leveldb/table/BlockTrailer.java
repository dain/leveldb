package org.iq80.leveldb.table;

import com.google.common.base.Preconditions;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

public class BlockTrailer
{
    public static final int ENCODED_LENGTH = 5;

    private final CompressionType compressionType;
    private final int crc32c;

    public BlockTrailer(CompressionType compressionType, int crc32c)
    {
        Preconditions.checkNotNull(compressionType, "compressionType is null");

        this.compressionType = compressionType;
        this.crc32c = crc32c;
    }

    public CompressionType getCompressionType()
    {
        return compressionType;
    }

    public int getCrc32c()
    {
        return crc32c;
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

        BlockTrailer that = (BlockTrailer) o;

        if (crc32c != that.crc32c) {
            return false;
        }
        if (compressionType != that.compressionType) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = compressionType.hashCode();
        result = 31 * result + crc32c;
        return result;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("BlockTrailer");
        sb.append("{compressionType=").append(compressionType);
        sb.append(", crc32c=0x").append(Integer.toHexString(crc32c));
        sb.append('}');
        return sb.toString();
    }

    public static BlockTrailer readBlockTrailer(ChannelBuffer buffer)
    {
        CompressionType compressionType = CompressionType.getCompressionTypeByPersistentId(buffer.readUnsignedByte());
        int crc32c = buffer.readInt();
        return new BlockTrailer(compressionType, crc32c);
    }

    public static ChannelBuffer writeBlockTrailer(BlockTrailer blockTrailer)
    {
        ChannelBuffer buffer = ChannelBuffers.buffer(ENCODED_LENGTH);
        writeBlockTrailer(blockTrailer, buffer);
        return buffer;
    }

    public static void writeBlockTrailer(BlockTrailer blockTrailer, ChannelBuffer buffer)
    {
        buffer.writeByte(blockTrailer.getCompressionType().getPersistentId());
        buffer.writeInt(blockTrailer.getCrc32c());
    }
}
