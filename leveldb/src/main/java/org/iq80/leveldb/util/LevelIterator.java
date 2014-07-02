
package org.iq80.leveldb.util;

import org.iq80.leveldb.impl.FileMetaData;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.InternalKeyComparator;
import org.iq80.leveldb.impl.TableCache;

import java.util.List;
import java.util.Map.Entry;

import org.iq80.leveldb.util.TableIterator.CurrentOrigin;
import static org.iq80.leveldb.util.TableIterator.CurrentOrigin.*;

public final class LevelIterator extends AbstractReverseSeekingIterator<InternalKey, Slice>
      implements
         InternalIterator
{
   private final TableCache tableCache;
   private final List<FileMetaData> files;
   private final InternalKeyComparator comparator;
   private InternalTableIterator current;
   private CurrentOrigin currentOrigin = NONE; // see TableIterator for explanation of this enum's
// functionality
   private int index;

   public LevelIterator(
         TableCache tableCache,
         List<FileMetaData> files,
         InternalKeyComparator comparator)
   {
      this.tableCache = tableCache;
      this.files = files;
      this.comparator = comparator;
   }

   @Override
   protected void seekToFirstInternal()
   {
      // reset index to before first and clear the data iterator
      index = 0;
      current = null;
      currentOrigin = NONE;
   }

   @Override
   protected void seekToLastInternal()
   {
      index = files.size() - 1;
      current = openFile(index);
      currentOrigin = PREV;
      current.seekToLastInternal();
   }

   @Override
   public void seekToEndInternal()
   {
      index = files.size() - 1;
      current = openFile(index);
      currentOrigin = PREV;
      current.seekToEnd();
   }

   @Override
   protected void seekInternal(InternalKey targetKey)
   {
      // seek the index to the block containing the key
      if (files.size() == 0)
      {
         return;
      }

      // todo replace with Collections.binarySearch
      int left = 0;
      int right = files.size() - 1;

      // binary search restart positions to find the restart position immediately before the
// targetKey
      while (left < right)
      {
         int mid = (left + right) / 2;

         if (comparator.compare(files.get(mid).getLargest(), targetKey) < 0)
         {
            // Key at "mid.largest" is < "target". Therefore all
            // files at or before "mid" are uninteresting.
            left = mid + 1;
         }
         else
         {
            // Key at "mid.largest" is >= "target". Therefore all files
            // after "mid" are uninteresting.
            right = mid;
         }
      }
      index = right;

      // if the index is now pointing to the last block in the file, check if the largest key
      // in the block is less than the target key. If so, we need to seek beyond the end of this
// file
      if (index == files.size() - 1
            && comparator.compare(files.get(index).getLargest(), targetKey) < 0)
      {
         index++;
      }

      // if indexIterator does not have a next, it means the key does not exist in this iterator
      if (index < files.size())
      {
         // seek the current iterator to the key
         current = openNextFile();
         current.seek(targetKey);
      }
      else
      {
         current = null;
         currentOrigin = NONE;
      }
   }

   @Override
   protected boolean hasNextInternal()
   {
      return currentHasNext();
   }

   @Override
   protected boolean hasPrevInternal()
   {
      return currentHasPrev();
   }

   @Override
   protected Entry<InternalKey, Slice> getNextElement()
   {
      // note: it must be here & not where 'current' is assigned,
      // because otherwise we'll have called inputs.next() before throwing
      // the first NPE, and the next time around we'll call inputs.next()
      // again, incorrectly moving beyond the error.
      return currentHasNext() ? current.next() : null;
   }

   @Override
   protected Entry<InternalKey, Slice> getPrevElement()
   {
      return currentHasPrev() ? current.prev() : null;
   }

   @Override
   protected Entry<InternalKey, Slice> peekInternal()
   {
      return currentHasNext() ? current.peek() : null;
   }

   @Override
   protected Entry<InternalKey, Slice> peekPrevInternal()
   {
      return currentHasPrev() ? current.peekPrev() : null;
   }

   private boolean currentHasNext()
   {
      boolean currentHasNext = false;
      while (true)
      {
         if (current != null)
         {
            currentHasNext = current.hasNext();
         }
         if (!currentHasNext)
         {
            if (currentOrigin == PREV)
            {
               // current came from PREV, so the index currently points at the index of current's
// file
               // we want to check the next file, however
               index++;
            }
            if (index < files.size())
            {
               current = openNextFile();
            }
            else
            {
               break;
            }
         }
         else
         {
            break;
         }
      }
      if (!currentHasNext)
      {
         current = null;
         currentOrigin = NONE;
      }
      return currentHasNext;
   }

   private boolean currentHasPrev()
   {
      boolean currentHasPrev = false;
      while (true)
      {
         if (current != null)
         {
            currentHasPrev = current.hasPrev();
         }
         if (!currentHasPrev)
         {
            if (currentOrigin == NEXT)
            {
               index--;
            }
            if (index > 0)
            {
               current = openPrevFile();
               current.seekToEnd();
            }
            else
            {
               break;
            }
         }
         else
         {
            break;
         }
      }
      if (!currentHasPrev)
      {
         current = null;
         currentOrigin = NONE;
      }
      return currentHasPrev;
   }

   private InternalTableIterator openFile(int i)
   {
      return tableCache.newIterator(files.get(i));
   }

   private InternalTableIterator openNextFile()
   {
      currentOrigin = NEXT;
      return openFile(index++);
   }

   private InternalTableIterator openPrevFile()
   {
      currentOrigin = PREV;
      return openFile(--index);
   }

   @Override
   public String toString()
   {
      final StringBuilder sb = new StringBuilder();
      sb.append("ConcatenatingIterator");
      sb.append("{index=").append(index);
      sb.append(", files=").append(files);
      sb.append(", current=").append(current);
      sb.append('}');
      return sb.toString();
   }
}
