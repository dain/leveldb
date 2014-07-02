/**
 * Copyright (C) 2011 the original author or authors. See the notice.md file distributed with this
 * work for additional information regarding copyright ownership.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.iq80.leveldb.table;

import com.google.common.base.Preconditions;

import org.iq80.leveldb.impl.ReverseSeekingIterator;
import org.iq80.leveldb.util.SliceInput;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.Slices;
import org.iq80.leveldb.util.VariableLengthQuantity;
import org.iq80.leveldb.util.SliceOutput;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.NoSuchElementException;

import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;

public class BlockIterator implements ReverseSeekingIterator<Slice, Slice>
{
   private final SliceInput data;
   private final Slice restartPositions;
   private final int restartCount;
   private int prevPosition;
   private int restartIndex;
   private final Comparator<Slice> comparator;

   private BlockEntry nextEntry;
   private BlockEntry prevEntry;

   private final Deque<CacheEntry> prevCache;
   private int prevCacheRestartIndex;

   public BlockIterator(Slice data, Slice restartPositions, Comparator<Slice> comparator)
   {
      Preconditions.checkNotNull(data, "data is null");
      Preconditions.checkNotNull(restartPositions, "restartPositions is null");
      Preconditions.checkArgument(restartPositions.length() % SIZE_OF_INT == 0,
            "restartPositions.readableBytes() must be a multiple of %s", SIZE_OF_INT);
      Preconditions.checkNotNull(comparator, "comparator is null");

      this.data = data.input();

      this.restartPositions = restartPositions.slice();
      this.restartCount = this.restartPositions.length() / SIZE_OF_INT;

      this.comparator = comparator;

      prevCache = new ArrayDeque<CacheEntry>();
      prevCacheRestartIndex = -1;

      seekToFirst();
   }

   @Override
   public boolean hasNext()
   {
      return nextEntry != null;
   }

   @Override
   public boolean hasPrev()
   {
      return prevEntry != null || currentPosition() > 0;
   }

   @Override
   public BlockEntry peek()
   {
      if (!hasNext())
      {
         throw new NoSuchElementException();
      }
      return nextEntry;
   }

   @Override
   public BlockEntry peekPrev()
   {
      if (prevEntry == null && currentPosition() > 0)
      {
         // this case should only occur after seeking under certain conditions
         BlockEntry peeked = prev();
         next();
         prevEntry = peeked;
      }
      else if (prevEntry == null)
      {
         throw new NoSuchElementException();
      }
      return prevEntry;
   }

   @Override
   public BlockEntry next()
   {
      if (!hasNext())
      {
         throw new NoSuchElementException();
      }

      prevEntry = nextEntry;

      if (!data.isReadable())
      {
         nextEntry = null;
      }
      else
      {
         // read entry at current data position
         nextEntry = readEntry(data, prevEntry);
         resetCache();
      }
      return prevEntry;
   }

   @Override
   public BlockEntry prev()
   {
      int original = currentPosition();
      if (original == 0)
      {
         throw new NoSuchElementException();
      }

      int previousRestart = getPreviousRestart(restartIndex, original);
      if (previousRestart == prevCacheRestartIndex && prevCache.size() > 0)
      {
         CacheEntry prevState = prevCache.pop();
         nextEntry = prevState.entry;
         prevPosition = prevState.prevPosition;
         data.setPosition(prevState.dataPosition);

         CacheEntry peek = prevCache.peek();
         prevEntry = peek == null ? null : peek.entry;
      }
      else
      {
         seekToRestartPosition(previousRestart);
         prevCacheRestartIndex = previousRestart;
         prevCache.push(new CacheEntry(nextEntry, prevPosition, data.position()));
         while (data.position() < original && data.isReadable())
         {
            prevEntry = nextEntry;
            nextEntry = readEntry(data, prevEntry);
            prevCache.push(new CacheEntry(nextEntry, prevPosition, data.position()));
         }
         prevCache.pop(); // we don't want to cache the last entry because that's returned with this
// call
      }

      return nextEntry;
   }

   private int getPreviousRestart(int startIndex, int position)
   {
      while (getRestartPoint(startIndex) >= position)
      {
         if (startIndex == 0)
         {
            throw new NoSuchElementException();
         }
         startIndex--;
      }
      return startIndex;
   }

   private int currentPosition()
   {
      // lags data.position because of the nextEntry read-ahead
      if (nextEntry != null)
      {
         return prevPosition;
      }
      return data.position();
   }

   @Override
   public void remove()
   {
      throw new UnsupportedOperationException();
   }

   /**
    * Repositions the iterator to the beginning of this block.
    */
   @Override
   public void seekToFirst()
   {
      if (restartCount > 0)
      {
         seekToRestartPosition(0);
      }
   }

   @Override
   public void seekToLast()
   {
      if (restartCount > 0)
      {
         seekToRestartPosition(Math.max(0, restartCount - 1));
         while (data.isReadable())
         {
            // seek until reaching the last entry
            next();
         }
      }
   }

   @Override
   public void seekToEnd()
   {
      if (restartCount > 0)
      {
         // TODO might be able to accomplish with setting data position
         seekToLast();
         next();
      }
   }

   /**
    * Repositions the iterator so the key of the next BlockElement returned greater than or equal to
    * the specified targetKey.
    */
   @Override
   public void seek(Slice targetKey)
   {
      if (restartCount == 0)
      {
         return;
      }

      int left = 0;
      int right = restartCount - 1;

      // binary search restart positions to find the restart position immediately before the
// targetKey
      while (left < right)
      {
         int mid = (left + right + 1) / 2;

         seekToRestartPosition(mid);

         if (comparator.compare(nextEntry.getKey(), targetKey) < 0)
         {
            // key at mid is smaller than targetKey. Therefore all restart
            // blocks before mid are uninteresting.
            left = mid;
         }
         else
         {
            // key at mid is greater than or equal to targetKey. Therefore
            // all restart blocks at or after mid are uninteresting.
            right = mid - 1;
         }
      }

      // linear search (within restart block) for first key greater than or equal to targetKey
      for (seekToRestartPosition(left); nextEntry != null; next())
      {
         if (comparator.compare(peek().getKey(), targetKey) >= 0)
         {
            break;
         }
      }

   }

   /**
    * Seeks to and reads the entry at the specified restart position.
    * <p/>
    * After this method, nextEntry will contain the next entry to return, and the previousEntry will
    * be null.
    */
   private void seekToRestartPosition(int restartPosition)
   {
      Preconditions.checkPositionIndex(restartPosition, restartCount, "restartPosition");

      // seek data readIndex to the beginning of the restart block
      data.setPosition(getRestartPoint(restartPosition));

      // clear the entries to assure key is not prefixed
      nextEntry = null;
      prevEntry = null;

      resetCache();

      restartIndex = restartPosition;

      // read the entry
      nextEntry = readEntry(data, null);
   }

   private int getRestartPoint(int index)
   {
      return restartPositions.getInt(index * SIZE_OF_INT);
   }

   /**
    * Reads the entry at the current data readIndex. After this method, data readIndex is positioned
    * at the beginning of the next entry or at the end of data if there was not a next entry.
    * 
    * @return true if an entry was read
    */
   private BlockEntry readEntry(SliceInput data, BlockEntry previousEntry)
   {
      prevPosition = data.position();

      // read entry header
      int sharedKeyLength = VariableLengthQuantity.readVariableLengthInt(data);
      int nonSharedKeyLength = VariableLengthQuantity.readVariableLengthInt(data);
      int valueLength = VariableLengthQuantity.readVariableLengthInt(data);

      // read key
      Slice key = Slices.allocate(sharedKeyLength + nonSharedKeyLength);
      SliceOutput sliceOutput = key.output();
      if (sharedKeyLength > 0)
      {
         Preconditions.checkState(previousEntry != null,
               "Entry has a shared key but no previous entry was provided");
         sliceOutput.writeBytes(previousEntry.getKey(), 0, sharedKeyLength);
      }
      sliceOutput.writeBytes(data, nonSharedKeyLength);

      // read value
      Slice value = data.readSlice(valueLength);

      return new BlockEntry(key, value);
   }

   private void resetCache()
   {
      prevCache.clear();
      prevCacheRestartIndex = -1;
   }

   private static class CacheEntry
   {
      public final BlockEntry entry;
      public final int prevPosition, dataPosition;

      public CacheEntry(BlockEntry entry, int prevPosition, int dataPosition)
      {
         this.entry = entry;
         this.prevPosition = prevPosition;
         this.dataPosition = dataPosition;
      }
   }
}
