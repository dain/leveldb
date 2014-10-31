/*
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

import com.google.common.base.Preconditions;
import org.iq80.leveldb.impl.SeekingIterator;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.SliceInput;
import org.iq80.leveldb.util.SliceOutput;
import org.iq80.leveldb.util.Slices;
import org.iq80.leveldb.util.VariableLengthQuantity;

import java.util.Comparator;
import java.util.NoSuchElementException;

import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;

public class BlockIterator
        implements SeekingIterator<Slice, Slice>
{
    private final SliceInput data;
    private final Slice restartPositions;
    private final int restartCount;
    private final Comparator<Slice> comparator;

    private BlockEntry nextEntry;

    public BlockIterator(Slice data, Slice restartPositions, Comparator<Slice> comparator)
    {
        Preconditions.checkNotNull(data, "data is null");
        Preconditions.checkNotNull(restartPositions, "restartPositions is null");
        Preconditions.checkArgument(restartPositions.length() % SIZE_OF_INT == 0, "restartPositions.readableBytes() must be a multiple of %s", SIZE_OF_INT);
        Preconditions.checkNotNull(comparator, "comparator is null");

        this.data = data.input();

        this.restartPositions = restartPositions.slice();
        restartCount = this.restartPositions.length() / SIZE_OF_INT;

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

        if (!data.isReadable()) {
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
    public void seek(Slice targetKey)
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
        Preconditions.checkPositionIndex(restartPosition, restartCount, "restartPosition");

        // seek data readIndex to the beginning of the restart block
        int offset = restartPositions.getInt(restartPosition * SIZE_OF_INT);
        data.setPosition(offset);

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
    private static BlockEntry readEntry(SliceInput data, BlockEntry previousEntry)
    {
        Preconditions.checkNotNull(data, "data is null");

        // read entry header
        int sharedKeyLength = VariableLengthQuantity.readVariableLengthInt(data);
        int nonSharedKeyLength = VariableLengthQuantity.readVariableLengthInt(data);
        int valueLength = VariableLengthQuantity.readVariableLengthInt(data);

        // read key
        Slice key = Slices.allocate(sharedKeyLength + nonSharedKeyLength);
        SliceOutput sliceOutput = key.output();
        if (sharedKeyLength > 0) {
            Preconditions.checkState(previousEntry != null, "Entry has a shared key but no previous entry was provided");
            sliceOutput.writeBytes(previousEntry.getKey(), 0, sharedKeyLength);
        }
        sliceOutput.writeBytes(data, nonSharedKeyLength);

        // read value
        Slice value = data.readSlice(valueLength);

        return new BlockEntry(key, value);
    }
}
