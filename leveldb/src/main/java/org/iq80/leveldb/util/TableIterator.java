
package org.iq80.leveldb.util;

import org.iq80.leveldb.table.Block;
import org.iq80.leveldb.table.BlockIterator;
import org.iq80.leveldb.table.Table;

import java.util.Map.Entry;

import static org.iq80.leveldb.util.TableIterator.CurrentOrigin.*;

public final class TableIterator extends AbstractReverseSeekingIterator<Slice, Slice>
{
   private final Table table;
   private final BlockIterator blockIterator;
   private BlockIterator current;
   private CurrentOrigin currentOrigin = NONE;

   protected enum CurrentOrigin
   {
      /*
       * reversable iterators don't have a consistent concept of a "current" item instead they exist
       * in a position "between" next and prev. in order to make the BlockIterator 'current' work,
       * we need to track from which direction it was initialized so that calls to advance the
       * encompassing 'blockIterator' are consistent
       */
      PREV, NEXT, NONE
      // a state of NONE should be interchangeable with current==NULL
   }

   public TableIterator(Table table, BlockIterator blockIterator)
   {
      this.table = table;
      this.blockIterator = blockIterator;
      clearCurrent();
   }

   @Override
   protected void seekToFirstInternal()
   {
      // reset index to before first and clear the data iterator
      blockIterator.seekToFirst();
      clearCurrent();
   }

   @Override
   protected void seekToLastInternal()
   {
      blockIterator.seekToEnd();
      clearCurrent();
      if (currentHasPrev())
      {
         current.prev();
      }
   }

   @Override
   public void seekToEndInternal()
   {
      blockIterator.seekToEnd();
      clearCurrent();
   }

   @Override
   protected void seekInternal(Slice targetKey)
   {
      // seek the index to the block containing the key
      blockIterator.seek(targetKey);

      // if indexIterator does not have a next, it mean the key does not exist in this iterator
      if (blockIterator.hasNext())
      {
         // seek the current iterator to the key
         current = getNextBlock();
         current.seek(targetKey);
      }
      else
      {
         clearCurrent();
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
   protected Entry<Slice, Slice> getNextElement()
   {
      // note: it must be here & not where 'current' is assigned,
      // because otherwise we'll have called inputs.next() before throwing
      // the first NPE, and the next time around we'll call inputs.next()
      // again, incorrectly moving beyond the error.
      return currentHasNext() ? current.next() : null;
   }

   @Override
   protected Entry<Slice, Slice> getPrevElement()
   {
      return currentHasPrev() ? current.prev() : null;
   }

   @Override
   protected Entry<Slice, Slice> peekInternal()
   {
      return currentHasNext() ? current.peek() : null;
   }

   @Override
   protected Entry<Slice, Slice> peekPrevInternal()
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
               // current came from PREV, so advancing blockIterator to next() must be safe
               // indeed, because alternating calls to prev() and next() must return the same item
               // current can be retrieved from next() when the origin is PREV
               blockIterator.next();
               // but of course we want to go beyond current to the next block
               // so we pass into the next if
            }
            if (blockIterator.hasNext())
            {
               current = getNextBlock();
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
         clearCurrent();
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
         if (!(currentHasPrev))
         {
            if (currentOrigin == NEXT)
            {
               blockIterator.prev();
            }
            if (blockIterator.hasPrev())
            {
               current = getPrevBlock();
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
         clearCurrent();
      }
      return currentHasPrev;
   }

   private BlockIterator getNextBlock()
   {
      Slice blockHandle = blockIterator.next().getValue();
      Block dataBlock = table.openBlock(blockHandle);
      currentOrigin = NEXT;
      return dataBlock.iterator();
   }

   private BlockIterator getPrevBlock()
   {
      Slice blockHandle = blockIterator.prev().getValue();
      Block dataBlock = table.openBlock(blockHandle);
      currentOrigin = PREV;
      return dataBlock.iterator();
   }

   private void clearCurrent()
   {
      current = null;
      currentOrigin = NONE;
   }

   @Override
   public String toString()
   {
      final StringBuilder sb = new StringBuilder();
      sb.append("ConcatenatingIterator");
      sb.append("{blockIterator=").append(blockIterator);
      sb.append(", current=").append(current);
      sb.append('}');
      return sb.toString();
   }
}
