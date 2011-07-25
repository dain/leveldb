package org.iq80.leveldb.table;

import com.google.common.base.Preconditions;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import static org.iq80.leveldb.table.BlockHandle.readBlockHandle;
import static org.iq80.leveldb.table.BlockHandle.writeBlockHandle;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_LONG;

public class Footer
{
    public static final int ENCODED_LENGTH = (BlockHandle.MAX_ENCODED_LENGTH * 2) + SIZE_OF_LONG;

    private final BlockHandle metaindexBlockHandle;
    private final BlockHandle indexBlockHandle;

    Footer(BlockHandle metaindexBlockHandle, BlockHandle indexBlockHandle)
    {
        this.metaindexBlockHandle = metaindexBlockHandle;
        this.indexBlockHandle = indexBlockHandle;
    }

    public BlockHandle getMetaindexBlockHandle()
    {
        return metaindexBlockHandle;
    }

    public BlockHandle getIndexBlockHandle()
    {
        return indexBlockHandle;
    }

    public static Footer readFooter(ChannelBuffer buffer)
    {
        // remember the starting read index so we can calculate the padding
        int startingReadIndex = buffer.readerIndex();

        // read metaindex and index handles
        BlockHandle metaindexBlockHandle = readBlockHandle(buffer);
        BlockHandle indexBlockHandle = readBlockHandle(buffer);

        // skip padding
        buffer.readerIndex(startingReadIndex + ENCODED_LENGTH - SIZE_OF_LONG);

        // verify magic number
        long magicNumber = buffer.readUnsignedInt() | (buffer.readUnsignedInt() << 32);
        Preconditions.checkArgument(magicNumber == TableBuilder.TABLE_MAGIC_NUMBER, "File is not a table (bad magic number)");

        return new Footer(metaindexBlockHandle, indexBlockHandle);
    }

    public static ChannelBuffer writeFooter(Footer Footer)
    {
        ChannelBuffer buffer = ChannelBuffers.buffer(ENCODED_LENGTH);
        writeFooter(Footer, buffer);
        return buffer;
    }

    public static void writeFooter(Footer footer, ChannelBuffer buffer)
    {
        // remember the starting write index so we can calculate the padding
        int startingWriteIndex = buffer.writerIndex();

        // write metaindex and index handles
        writeBlockHandle(footer.getMetaindexBlockHandle(), buffer);
        writeBlockHandle(footer.getIndexBlockHandle(), buffer);

        // write padding
        buffer.writeZero(ENCODED_LENGTH - SIZE_OF_LONG - (buffer.writerIndex() - startingWriteIndex));

        // write magic number as two (little endian) integers
        buffer.writeInt((int) TableBuilder.TABLE_MAGIC_NUMBER);
        buffer.writeInt((int) (TableBuilder.TABLE_MAGIC_NUMBER >> 32));
    }

}
