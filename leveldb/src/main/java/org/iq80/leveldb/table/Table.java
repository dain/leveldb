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
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.Slices;
import org.iq80.leveldb.util.SliceOutput;
import org.iq80.leveldb.util.VariableLengthQuantity;
import org.xerial.snappy.Snappy;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Comparator;

import static org.iq80.leveldb.CompressionType.SNAPPY;

public class Table implements SeekingIterable<Slice, Slice>
{
    private final String name;
    private final FileChannel fileChannel;
    private final Comparator<Slice> comparator;

    private final boolean verifyChecksums;

    private final Block indexBlock;
    private final BlockHandle metaindexBlockHandle;

    public Table(String name, FileChannel fileChannel, Comparator<Slice> comparator, boolean verifyChecksums)
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

        Slice footerSlice = read(fileChannel, fileChannel.size() - Footer.ENCODED_LENGTH, Footer.ENCODED_LENGTH);
        Footer footer = Footer.readFooter(footerSlice);

        indexBlock = readBlock(footer.getIndexBlockHandle());
        metaindexBlockHandle = footer.getMetaindexBlockHandle();
    }

    @Override
    public SeekingIterator<Slice, Slice> iterator()
    {
        SeekingIterator<Slice, BlockIterator> inputs = SeekingIterators.transformValues(indexBlock.iterator(), new Function<Slice, BlockIterator>()
        {
            @Override
            public BlockIterator apply(Slice blockEntry)
            {
                BlockHandle blockHandle = BlockHandle.readBlockHandle(blockEntry.input());
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
        Slice data = read(fileChannel, blockHandle.getOffset(), blockHandle.getFullBlockSize());

        // read block trailer
        BlockTrailer blockTrailer = BlockTrailer.readBlockTrailer(data.slice(blockHandle.getDataSize(), BlockTrailer.ENCODED_LENGTH));

        // only verify check sums if explicitly asked by the user
        if (verifyChecksums) {
            // checksum data and the compression type in the trailer
            PureJavaCrc32C checksum = new PureJavaCrc32C();
            checksum.update(data.getRawArray(), data.getRawOffset(), blockHandle.getDataSize() + 1);
            int actualCrc32c = checksum.getMaskedValue();

            Preconditions.checkState(blockTrailer.getCrc32c() == actualCrc32c, "Block corrupted: checksum mismatch");
        }

        // decompress data
        Slice uncompressedData;
        if (blockTrailer.getCompressionType() == SNAPPY) {
            uncompressedData = Slices.allocate(uncompressedLength(data));
            // todo when code is change to direct buffers, use the buffer method instead
            Snappy.uncompress(data.getRawArray(), data.getRawOffset(), blockHandle.getDataSize(), uncompressedData.getRawArray(), uncompressedData.getRawOffset());
        }
        else {
            uncompressedData = data.slice(0, blockHandle.getDataSize());
        }

        return new Block(uncompressedData, comparator);
    }

    private int uncompressedLength(Slice data)
            throws IOException
    {
        int length = VariableLengthQuantity.readVariableLengthInt(data.input());
        return length;
    }

    /**
     * Given a key, return an approximate byte offset in the file where
     * the data for that key begins (or would begin if the key were
     * present in the file).  The returned value is in terms of file
     * bytes, and so includes effects like compression of the underlying data.
     * For example, the approximate offset of the last key in the table will
     * be close to the file length.
     */
    public long getApproximateOffsetOf(Slice key)
    {
        BlockIterator iterator = indexBlock.iterator();
        iterator.seek(key);
        if (iterator.hasNext()) {
            BlockHandle blockHandle = BlockHandle.readBlockHandle(iterator.next().getValue().input());
            return blockHandle.getOffset();
        }

        // key is past the last key in the file.  Approximate the offset
        // by returning the offset of the metaindex block (which is
        // right near the end of the file).
        return metaindexBlockHandle.getOffset();
    }


    public static Slice read(FileChannel channel, long position, int length)
            throws IOException
    {
        SliceOutput buffer = Slices.allocate(length).output();

        while (buffer.writableBytes() > 0) {
            int bytesRead = buffer.writeBytes(channel, (int) position, buffer.writableBytes());
            if (bytesRead < 0) {
                // error tried to read off the end of the file
                throw new EOFException();
            }

        }

        return buffer.slice();
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
