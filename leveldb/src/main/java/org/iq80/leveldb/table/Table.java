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
package org.iq80.leveldb.table;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.iq80.leveldb.impl.SeekingIterable;
import org.iq80.leveldb.impl.SeekingIterator;
import org.iq80.leveldb.util.PureJavaCrc32C;
import org.iq80.leveldb.util.SeekingIterators;
import org.jboss.netty.buffer.ChannelBuffer;
import org.iq80.leveldb.util.Buffers;
import org.xerial.snappy.Snappy;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;

import static org.iq80.leveldb.CompressionType.SNAPPY;

public class Table implements SeekingIterable<ChannelBuffer, ChannelBuffer>
{
    private final String name;
    private final FileChannel fileChannel;
    private final Comparator<ChannelBuffer> comparator;

    private final boolean verifyChecksums;

    private final Block indexBlock;
    private final BlockHandle metaindexBlockHandle;

    public Table(String name, FileChannel fileChannel, Comparator<ChannelBuffer> comparator, boolean verifyChecksums)
            throws IOException
    {
        Preconditions.checkNotNull(name, "name is null");
        Preconditions.checkNotNull(fileChannel, "fileChannel is null");
        Preconditions.checkArgument(fileChannel.size() >= Footer.ENCODED_LENGTH, "File is corrupt: size must be at least %s bytes", Footer.ENCODED_LENGTH);
        Preconditions.checkNotNull(comparator, "comparator is null");

        this.name = name;
        this.fileChannel = fileChannel;
        this.verifyChecksums = verifyChecksums;
        this.comparator = comparator;

        ChannelBuffer footerBuffer = read(fileChannel, fileChannel.size() - Footer.ENCODED_LENGTH, Footer.ENCODED_LENGTH);
        Footer footer = Footer.readFooter(footerBuffer);

        indexBlock = readBlock(footer.getIndexBlockHandle());
        metaindexBlockHandle = footer.getMetaindexBlockHandle();
    }

    @Override
    public SeekingIterator<ChannelBuffer, ChannelBuffer> iterator()
    {
        SeekingIterator<ChannelBuffer, BlockIterator> inputs = SeekingIterators.transformValues(indexBlock.iterator(), new Function<ChannelBuffer, BlockIterator>()
        {
            @Override
            public BlockIterator apply(ChannelBuffer blockEntry)
            {
                BlockHandle blockHandle = BlockHandle.readBlockHandle(blockEntry);
                try {
                    Block dataBlock = readBlock(blockHandle);
                    return dataBlock.iterator();
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        });

        return SeekingIterators.concat(inputs);
    }

    private Block readBlock(BlockHandle blockHandle)
            throws IOException
    {
        // read full block (data + trailer) into memory
        ChannelBuffer data = read(fileChannel, blockHandle.getOffset(), blockHandle.getFullBlockSize());

        // read block trailer
        BlockTrailer blockTrailer = BlockTrailer.readBlockTrailer(data.slice(blockHandle.getDataSize(), BlockTrailer.ENCODED_LENGTH));

        // only verify check sums if explicitly asked by the user
        if (verifyChecksums) {
            // checksum data and the compression type in the trailer
            PureJavaCrc32C checksum = new PureJavaCrc32C();
            checksum.update(data.array(), data.arrayOffset(), blockHandle.getDataSize() + 1);
            int actualCrc32c = checksum.getMaskedValue();

            Preconditions.checkState(blockTrailer.getCrc32c() == actualCrc32c, "Block corrupted: checksum mismatch");
        }

        // decompress data
        ChannelBuffer uncompressedData;
        if (blockTrailer.getCompressionType() == SNAPPY) {
            // todo when code is change to direct buffers, use the buffer method instead
            int uncompressedLength = Snappy.uncompressedLength(data.array(), data.arrayOffset() + data.readerIndex(), blockHandle.getDataSize());
            uncompressedData = Buffers.buffer(uncompressedLength);
            Snappy.uncompress(data.array(), data.arrayOffset() + data.readerIndex(), blockHandle.getDataSize(), uncompressedData.array(), uncompressedData.arrayOffset());
            uncompressedData.writerIndex(uncompressedLength);
        }
        else {
            uncompressedData = data.slice(data.readerIndex(), blockHandle.getDataSize());
        }

        return new Block(uncompressedData, comparator);
    }

    /**
     * Given a key, return an approximate byte offset in the file where
     * the data for that key begins (or would begin if the key were
     * present in the file).  The returned value is in terms of file
     * bytes, and so includes effects like compression of the underlying data.
     * For example, the approximate offset of the last key in the table will
     * be close to the file length.
     */
    public long getApproximateOffsetOf(ChannelBuffer key)
    {
        BlockIterator iterator = indexBlock.iterator();
        iterator.seek(key);
        if (iterator.hasNext()) {
            BlockHandle blockHandle = BlockHandle.readBlockHandle(iterator.next().getValue());
            return blockHandle.getOffset();
        }

        // key is past the last key in the file.  Approximate the offset
        // by returning the offset of the metaindex block (which is
        // right near the end of the file).
        return metaindexBlockHandle.getOffset();
    }


    public static ChannelBuffer read(FileChannel channel, long position, int length)
            throws IOException
    {
        ByteBuffer buffer = Buffers.allocateByteBuffer(length);

        while (buffer.remaining() > 0) {
            int bytesRead = channel.read(buffer, position + buffer.position());
            if (bytesRead < 0) {
                // error tried to read off the end of the file
                throw new EOFException();
            }

        }
        buffer.position(0);

        return Buffers.wrappedBuffer(buffer);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("Table");
        sb.append("{name='").append(name).append('\'');
        sb.append(", comparator=").append(comparator);
        sb.append(", verifyChecksums=").append(verifyChecksums);
        sb.append('}');
        return sb.toString();
    }
}
