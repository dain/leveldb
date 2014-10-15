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

package org.iq80.leveldb.impl;

import com.google.common.collect.Maps;

import org.iq80.leveldb.util.AbstractReverseSeekingIterator;
import org.iq80.leveldb.util.DbIterator;
import org.iq80.leveldb.util.Slice;

import java.util.Comparator;
import java.util.Map.Entry;

import static org.iq80.leveldb.impl.SnapshotSeekingIterator.Direction.*;

public final class SnapshotSeekingIterator extends AbstractReverseSeekingIterator<Slice, Slice>
{
   private final DbIterator iterator;
   private final SnapshotImpl snapshot;
   private final Comparator<Slice> userComparator;
   // indicates the direction in which the iterator was last advanced
   private Direction direction;
   private Entry<InternalKey, Slice> savedEntry;

   protected enum Direction
   {
      FORWARD, REVERSE
   }

   public SnapshotSeekingIterator(
         DbIterator iterator,
         SnapshotImpl snapshot,
         Comparator<Slice> userComparator)
   {
      this.iterator = iterator;
      this.snapshot = snapshot;
      this.userComparator = userComparator;
      this.snapshot.getVersion().retain();
      this.savedEntry = null;
      seekToFirst();
   }

   public void close()
   {
      this.snapshot.getVersion().release();
   }

   @Override
   protected void seekToFirstInternal()
   {
      iterator.seekToFirst();
      direction = FORWARD;
   }

   @Override
   protected void seekToLastInternal()
   {
      seekToEndInternal();
      getPrevElement();
   }

   @Override
   public void seekToEndInternal()
   {
      iterator.seekToEnd();
      savedEntry = null;
      direction = REVERSE;
   }

   @Override
   protected void seekInternal(Slice targetKey)
   {
      iterator.seek(new InternalKey(targetKey, snapshot.getLastSequence(), ValueType.VALUE));
      findNextUserEntry(false, null);
      direction = REVERSE; // the next user entry has been found, but not yet advanced

   }

   @Override
   protected Entry<Slice, Slice> getNextElement()
   {
      if (direction == REVERSE)
      {
         if (!iterator.hasNext())
         {
            savedEntry = null;
            return null;
         }
         direction = FORWARD;

         // the last valid entry was returned by getPrevElement
         // so iterator's next must be the valid entry
      }
      else
      {
         findNextUserEntry(true, savedEntry);

         if (!iterator.hasNext())
         {
            return null;
         }
      }

      savedEntry = iterator.next();

      return Maps.immutableEntry(savedEntry.getKey().getUserKey(), savedEntry.getValue());
   }

   @Override
   protected Entry<Slice, Slice> getPrevElement()
   {
      if (direction == FORWARD)
      {
         if (!iterator.hasPrev())
         {
            savedEntry = null;
            return null;
         }
         direction = REVERSE;
         // the last valid entry was returned by getNextElement
         // so iterator's prev must be the valid entry
         savedEntry = iterator.prev();
      }
      else
      {
         findPrevUserEntry();

         if (savedEntry == null)
         {
            return null;
         }
      }

      return Maps.immutableEntry(savedEntry.getKey().getUserKey(), savedEntry.getValue());
   }

   @Override
   protected Entry<Slice, Slice> peekInternal()
   {
      if (hasNextInternal())
      {
         Entry<InternalKey, Slice> peek = iterator.peek();
         return Maps.immutableEntry(peek.getKey().getUserKey(), peek.getValue());
      }
      return null;
   }

   @Override
   protected Entry<Slice, Slice> peekPrevInternal()
   {
      if (hasPrevInternal())
      {
         Entry<InternalKey, Slice> peekPrev = iterator.peekPrev();
         return Maps.immutableEntry(peekPrev.getKey().getUserKey(), peekPrev.getValue());
      }
      return null;
   }

   private void findNextUserEntry(boolean skipping, Entry<InternalKey, Slice> skipEntry)
   {
      Slice skipKey;
      if (skipEntry == null)
      {
         skipping = false;
         skipKey = null;
      }
      else{
         skipKey = skipEntry.getKey().getUserKey();
      }
      
      while (iterator.hasNext())
      {
         InternalKey internalKey = iterator.peek().getKey();
         if (internalKey.getSequenceNumber() <= snapshot.getLastSequence())
         {
            switch (internalKey.getValueType())
            {
               case DELETION :
                  skipKey = internalKey.getUserKey();
                  skipping = true;
                  break;
               case VALUE :
                  if (!skipping || userComparator.compare(internalKey.getUserKey(), skipKey) > 0)
                  {
                     savedEntry = null;
                     return;
                  }
                  break;
            }
         }
         iterator.next();
      }
      savedEntry = null;
   }

   private void findPrevUserEntry()
   {
      ValueType valueType = ValueType.DELETION;
      while (iterator.hasPrev())
      {
         Entry<InternalKey, Slice> peekPrev = iterator.peekPrev();
         InternalKey internalKey = peekPrev.getKey();
         if (internalKey.getSequenceNumber() <= snapshot.getLastSequence())
         {
            if (valueType != ValueType.DELETION
                  && (savedEntry == null || userComparator.compare(internalKey.getUserKey(),
                        savedEntry.getKey().getUserKey()) < 0))
            {
               break;
            }
            valueType = internalKey.getValueType();
            if (valueType == ValueType.DELETION)
            {
               savedEntry = null;
            }
            else
            {
               savedEntry = peekPrev;
            }
         }
         else if(valueType == ValueType.VALUE){
            //we've found an entry out of this sequence after finding a value type
            //the value type is a valid entry to return, stop advancing prev
            return;
         }
         iterator.prev();
      }

      if (valueType == ValueType.DELETION)
      {
         savedEntry = null;
         direction = FORWARD;
      }
   }

   @Override
   public String toString()
   {
      final StringBuilder sb = new StringBuilder();
      sb.append("SnapshotSeekingIterator");
      sb.append("{snapshot=").append(snapshot);
      sb.append(", iterator=").append(iterator);
      sb.append('}');
      return sb.toString();
   }

   @Override
   protected boolean hasNextInternal()
   {
      if (direction == FORWARD)
      {
         findNextUserEntry(true, savedEntry);
         // calls to findNextUserEntry will place the iterator in a state where
         // next() is valid, which is the same as a state of coming from a reverse advance
         direction = REVERSE;
      }
      return iterator.hasNext();
   }

   @Override
   protected boolean hasPrevInternal()
   {
      if (direction == REVERSE)
      {
         findPrevUserEntry();
         if (savedEntry != null)
         {
            // findPrevUserEntry places the iterator before the valid user entry
            // so hasPrev after this call is answered by hasNext
            // but the has... functions should not advance the iterator
            // so advance forward to a position that appears externally the same as before this call
            // (though not identical if deletions are present)
            iterator.next();
            direction = FORWARD;
         }
      }
      return iterator.hasPrev();
   }

}
