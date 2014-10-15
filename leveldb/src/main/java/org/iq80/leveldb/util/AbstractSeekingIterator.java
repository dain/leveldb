
package org.iq80.leveldb.util;

import org.iq80.leveldb.impl.SeekingIterator;

import java.util.Map.Entry;
import java.util.NoSuchElementException;

public abstract class AbstractSeekingIterator<K, V> implements SeekingIterator<K, V>
{
   protected Entry<K, V> nextElement;

   @Override
   public void seekToFirst()
   {
      nextElement = null;
      seekToFirstInternal();
   }

   @Override
   public void seek(K targetKey)
   {
      nextElement = null;
      seekInternal(targetKey);
   }

   @Override
   public boolean hasNext()
   {
      if (nextElement == null)
      {
         nextElement = getNextElement();
      }
      return nextElement != null;
   }

   @Override
   public Entry<K, V> next()
   {
      if (nextElement == null)
      {
         nextElement = getNextElement();
         if (nextElement == null)
         {
            throw new NoSuchElementException();
         }
      }

      Entry<K, V> result = nextElement;
      nextElement = null;
      return result;
   }

   @Override
   public Entry<K, V> peek()
   {
      if (nextElement == null)
      {
         nextElement = getNextElement();
         if (nextElement == null)
         {
            throw new NoSuchElementException();
         }
      }

      return nextElement;
   }

   @Override
   public void remove()
   {
      throw new UnsupportedOperationException();
   }

   protected abstract void seekToFirstInternal();

   protected abstract void seekInternal(K targetKey);

   protected abstract Entry<K, V> getNextElement();
}
