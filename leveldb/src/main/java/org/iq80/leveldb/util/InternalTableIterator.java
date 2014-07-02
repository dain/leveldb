
package org.iq80.leveldb.util;

import com.google.common.collect.Maps;

import org.iq80.leveldb.impl.InternalKey;

import java.util.Map.Entry;

public class InternalTableIterator extends AbstractReverseSeekingIterator<InternalKey, Slice>
      implements
         InternalIterator
{
   private final TableIterator tableIterator;

   public InternalTableIterator(TableIterator tableIterator)
   {
      this.tableIterator = tableIterator;
   }

   @Override
   protected void seekToFirstInternal()
   {
      tableIterator.seekToFirst();
   }

   @Override
   protected void seekToLastInternal()
   {
      tableIterator.seekToLast();
   }

   @Override
   public void seekInternal(InternalKey targetKey)
   {
      tableIterator.seek(targetKey.encode());
   }

   @Override
   public void seekToEndInternal()
   {
      tableIterator.seekToEnd();
   }

   @Override
   protected Entry<InternalKey, Slice> getNextElement()
   {
      if (tableIterator.hasNext())
      {
         Entry<Slice, Slice> next = tableIterator.next();
         return Maps.immutableEntry(new InternalKey(next.getKey()), next.getValue());
      }
      return null;
   }

   @Override
   protected Entry<InternalKey, Slice> getPrevElement()
   {
      if (tableIterator.hasPrev())
      {
         Entry<Slice, Slice> prev = tableIterator.prev();
         return Maps.immutableEntry(new InternalKey(prev.getKey()), prev.getValue());
      }
      return null;
   }

   @Override
   protected Entry<InternalKey, Slice> peekInternal()
   {
      if (tableIterator.hasNext())
      {
         Entry<Slice, Slice> peek = tableIterator.peek();
         return Maps.immutableEntry(new InternalKey(peek.getKey()), peek.getValue());
      }
      return null;
   }

   @Override
   protected Entry<InternalKey, Slice> peekPrevInternal()
   {
      if (tableIterator.hasPrev())
      {
         Entry<Slice, Slice> peekPrev = tableIterator.peekPrev();
         return Maps.immutableEntry(new InternalKey(peekPrev.getKey()), peekPrev.getValue());
      }
      return null;
   }

   @Override
   public String toString()
   {
      final StringBuilder sb = new StringBuilder();
      sb.append("InternalTableIterator");
      sb.append("{fromIterator=").append(tableIterator);
      sb.append('}');
      return sb.toString();
   }

   @Override
   protected boolean hasNextInternal()
   {
      return tableIterator.hasNext();
   }

   @Override
   protected boolean hasPrevInternal()
   {
      return tableIterator.hasPrev();
   }
}
