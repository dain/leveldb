package org.iq80.leveldb.table;

import com.google.common.base.Preconditions;
import com.google.common.collect.PeekingIterator;
import org.iq80.leveldb.util.VariableLengthQuantity;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.util.Comparator;
import java.util.NoSuchElementException;

import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;

public class BlockIterator implements SeekingIterator
{
    private final ChannelBuffer data;
    private final ChannelBuffer restartPositions;
    private final int restartCount;
    private final Comparator<ChannelBuffer> comparator;

    private BlockEntry nextEntry;

    public BlockIterator(ChannelBuffer data, ChannelBuffer restartPositions, Comparator<ChannelBuffer> comparator)
    {
        Preconditions.checkNotNull(data, "data is null");
        Preconditions.checkNotNull(restartPositions, "restartPositions is null");
        Preconditions.checkArgument(restartPositions.readableBytes() % SIZE_OF_INT == 0, "restartPositions.readableBytes() must be a multiple of %s", SIZE_OF_INT);
        Preconditions.checkNotNull(comparator, "comparator is null");

        this.data = data.slice();

        this.restartPositions = restartPositions.slice();
        restartCount = this.restartPositions.readableBytes() / SIZE_OF_INT;

        this.comparator = comparator;

        seekToFirst();
    }

    @Override
    public boolean hasNext()
    {
        return nextEntry != null;
    }

    @Override
    public BlockEntry peek()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return nextEntry;
    }

    @Override
    public BlockEntry next()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        BlockEntry entry = nextEntry;

        if (!data.readable()) {
            nextEntry = null;
        }
        else {
            // read entry at current data position
            nextEntry = readEntry(data, nextEntry);
        }

        return entry;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Repositions the iterator so the beginning of this block.
     */
    @Override
    public void seekToFirst()
    {
        if (restartCount > 0) {
            seekToRestartPosition(0);
        }
    }

    /**
     * Repositions the iterator so the key of the next BlockElement returned greater than or equal to the specified targetKey.
     */
    @Override
    public void seek(ChannelBuffer targetKey)
    {
        if (restartCount == 0) {
            return;
        }

        int left = 0;
        int right = restartCount - 1;

        // binary search restart positions to find the restart position immediately before the targetKey
        while (left < right) {
            int mid = (left + right + 1) / 2;

            seekToRestartPosition(mid);

            if (comparator.compare(nextEntry.getKey(), targetKey) < 0) {
                // key at mid is smaller than targetKey.  Therefore all restart
                // blocks before mid are uninteresting.
                left = mid;
            }
            else {
                // key at mid is greater than or equal to targetKey.  Therefore
                // all restart blocks at or after mid are uninteresting.
                right = mid - 1;
            }
        }

        // linear search (within restart block) for first key greater than or equal to targetKey
        for (seekToRestartPosition(left); nextEntry != null; next()) {
            if (comparator.compare(peek().getKey(), targetKey) >= 0) {
                break;
            }
        }

    }

    /**
     * Seeks to and reads the entry at the specified restart position.
     * <p/>
     * After this method, nextEntry will contain the next entry to return, and the previousEntry will be null.
     */
    private void seekToRestartPosition(int restartPosition)
    {
        Preconditions.checkElementIndex(restartPosition, restartCount, "restartPosition");

        // seek data readIndex to the beginning of the restart block
        int offset = restartPositions.getInt(restartPosition * SIZE_OF_INT);
        data.readerIndex(offset);

        // clear the entries to assure key is not prefixed
        nextEntry = null;

        // read the entry
        nextEntry = readEntry(data, null);
    }

    /**
     * Reads the entry at the current data readIndex.
     * After this method, data readIndex is positioned at the beginning of the next entry
     * or at the end of data if there was not a next entry.
     *
     * @return true if an entry was read
     */
    private static BlockEntry readEntry(ChannelBuffer data, BlockEntry previousEntry)
    {
        Preconditions.checkNotNull(data, "data is null");

        // read entry header
        int sharedKeyLength = VariableLengthQuantity.unpackInt(data);
        int nonSharedKeyLength = VariableLengthQuantity.unpackInt(data);
        int valueLength = VariableLengthQuantity.unpackInt(data);

        // read key
        ChannelBuffer key = ChannelBuffers.buffer(sharedKeyLength + nonSharedKeyLength);
        if (sharedKeyLength > 0) {
            Preconditions.checkState(previousEntry != null, "Entry has a shared key but no previous entry was provided");
            key.writeBytes(previousEntry.getKey(), 0, sharedKeyLength);
        }
        key.writeBytes(data, nonSharedKeyLength);

        // read value
        ChannelBuffer value = data.readSlice(valueLength);

        return new BlockEntry(key, value);
    }
}
