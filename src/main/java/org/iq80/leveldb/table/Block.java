package org.iq80.leveldb.table;

import com.google.common.base.Preconditions;
import org.iq80.leveldb.SeekingIterable;
import org.jboss.netty.buffer.ChannelBuffer;
import org.iq80.leveldb.util.Buffers;

import java.util.Comparator;

import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;

/**
 * Binary Structure
 * <table summary="record format">
 * <tbody>
 * <thead>
 * <tr>
 * <th>name</th>
 * <th>offset</th>
 * <th>length</th>
 * <th>description</th>
 * </tr>
 * </thead>
 * <p/>
 * <tr>
 * <td>entries</td>
 * <td>4</td>
 * <td>vary</td>
 * <td>Entries in order by key</td>
 * </tr>
 * <tr>
 * <td>restart index</td>
 * <td>vary</td>
 * <td>4 * restart count</td>
 * <td>Index of prefix compression restarts</td>
 * </tr>
 * <tr>
 * <td>restart count</td>
 * <td>0</td>
 * <td>4</td>
 * <td>Number of prefix compression restarts (used as index into entries)</td>
 * </tr>
 * </tbody>
 * </table>
 */
public class Block implements SeekingIterable<ChannelBuffer, ChannelBuffer>
{
    private final ChannelBuffer block;
    private final Comparator<ChannelBuffer> comparator;

    private final ChannelBuffer data;
    private final ChannelBuffer restartPositions;

    public Block(ChannelBuffer block, Comparator<ChannelBuffer> comparator)
    {
        Preconditions.checkNotNull(block, "block is null");
        Preconditions.checkArgument(block.capacity() >= SIZE_OF_INT, "Block is corrupt: size must be at least %s block", SIZE_OF_INT);
        Preconditions.checkNotNull(comparator, "comparator is null");

        block = block.slice();
        this.block = block;
        this.comparator = comparator;

        // Keys are prefix compressed.  Every once in a while the prefix compression is restarted and the full key is written.
        // These "restart" locations are written at the end of the file, so you can seek to key without having to read the
        // entire file sequentially.

        // key restart count is the last int of the block
        int restartCount = block.getInt(block.capacity() - SIZE_OF_INT);

        if (restartCount > 0) {
            // restarts are written at the end of the block
            int restartOffset = block.readableBytes() - (1 + restartCount) * SIZE_OF_INT;
            Preconditions.checkArgument(restartOffset < block.readableBytes() - SIZE_OF_INT, "Block is corrupt: restart offset count is greater than block size");
            restartPositions = block.slice(restartOffset, restartCount * SIZE_OF_INT);

            // data starts at 0 and extends to the restart index
            data = block.slice(0, restartOffset);
        }
        else {
            data = Buffers.EMPTY_BUFFER;
            restartPositions = Buffers.EMPTY_BUFFER;
        }
    }

    public long size()
    {
        return block.capacity();
    }

    @Override
    public BlockIterator iterator()
    {
        return new BlockIterator(data, restartPositions, comparator);
    }

}
