
package org.iq80.leveldb.util;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import junit.framework.TestCase;
import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.impl.ReverseIterator;
import org.iq80.leveldb.impl.ReverseIterators;
import org.iq80.leveldb.impl.ReversePeekingIterator;
import org.iq80.leveldb.impl.ReverseSeekingIterator;

import com.google.common.collect.Maps;

import static com.google.common.base.Charsets.UTF_8;

public class DBIteratorTest extends TestCase
{
   private static final List<Entry<String, String>> entries;
   private final Options options = new Options().createIfMissing(true);
   private DB db;
   private File tempDir;

   static
   {
      Random rand = new Random(0);

      entries = new ArrayList<Entry<String, String>>();
      int items = 1000000;
      for (int i = 0; i < items; i++)
      {
         StringBuilder sb = new StringBuilder();
         for (int j = 0; j < 30; j++)
         {
            sb.append((char) ('a' + rand.nextInt(26)));
         }
         entries.add(Maps.immutableEntry(sb.toString(), "v:" + sb.toString()));
      }
   }

   @Override
   protected void setUp() throws Exception
   {
      tempDir = FileUtils.createTempDir("java-leveldb-testing-temp");
      db = factory.open(tempDir, options);
   }

   @Override
   protected void tearDown() throws Exception
   {
      try
      {
         db.close();
      }
      finally
      {
         FileUtils.deleteRecursively(tempDir);
      }
   }

   public void testForwardIteration()
   {
      putAll(db, entries);

      Collections.sort(entries, new StringDbIterator.EntryCompare());

      StringDbIterator actual = new StringDbIterator(db.iterator());
      int i = 0;
      for (Entry<String, String> expected : entries)
      {
         assertTrue(actual.hasNext());
         Entry<String, String> p = actual.peek();
         assertEquals("Item #" + i + " peek mismatch", expected, p);
         Entry<String, String> n = actual.next();
         assertEquals("Item #" + i + " mismatch", expected, n);
         i++;
      }
   }

   public void testReverseIteration()
   {
      putAll(db, entries);

      Collections.sort(entries, new StringDbIterator.ReverseEntryCompare());

      StringDbIterator actual = new StringDbIterator(db.iterator());
      actual.seekToLast();
      actual.next();
      int i = 0;
      for (Entry<String, String> expected : entries)
      {
         assertTrue(actual.hasPrev());
         Entry<String, String> p = actual.peekPrev();
         assertEquals("Item #" + i + " peek mismatch", expected, p);
         Entry<String, String> n = actual.prev();
         assertEquals("Item #" + i + " mismatch", expected, n);
         i++;
      }
   }

   public void testMixedIteration()
   {
      Random rand = new Random(0);

      putAll(db, entries);

      Collections.sort(entries, new StringDbIterator.EntryCompare());

      ReversePeekingIterator<Entry<String, String>> expected =
            ReverseIterators.reversePeekingIterator(entries);
      ReverseSeekingIterator<String, String> actual = new StringDbIterator(db.iterator());

      // take mixed forward and backward steps up the list then down the list (favoring forward to
// reach the end, then backward)
      int pos = 0;
      int randForward = 12, randBack = 4;// [-4, 7] inclusive, initially favor forward steps
      int steps = randForward + 1;
      do
      {
         int direction = steps < 0 ? -1 : 1; // mathematical sign for addition
         for (int i = 0; Math.abs(i) < Math.abs(steps); i += direction)
         {
            // if the expected iterator has items in this direction, proceed
            if (hasFollowing(direction, expected))
            {
               assertTrue(hasFollowing(direction, actual));

               assertEquals("Item #" + pos + " mismatch",
                     peekFollowing(direction, expected),
                     peekFollowing(direction, actual));

               assertEquals("Item #" + pos + " mismatch",
                     getFollowing(direction, expected),
                     getFollowing(direction, actual));

               pos += direction;
            }
            else
               break;
         }
         if (pos >= entries.size())
         {
            // switch to favor backward steps
            randForward = 4;
            randBack = 12;
            // [-7, 4] inclusive
         }
         steps = rand.nextInt(randForward) - randBack;
      } while (pos > 0);
   }
   
   public void testSeekPastContentsWithDeletesAndReverse(){
      String keys[] = {"a","b","c","d","e","f"};
      int deleteIndex[] = {2, 3, 5};

      List<Entry<String, String>> keyvals = new ArrayList<Entry<String, String>>();
      for(String key:keys){
         keyvals.add(Maps.immutableEntry(key, "v"+key));
      }
      
      for(int i = 0, d = 0; i < keyvals.size(); i++){
         Entry<String, String> e = keyvals.get(i);
         db.put(e.getKey().getBytes(UTF_8), e.getValue().getBytes(UTF_8));
         if(d < deleteIndex.length && i == deleteIndex[d]){
            db.delete(e.getKey().getBytes(UTF_8));
            d++;
         }
      }
      
      Set<Entry<String, String>> expectedSet = new TreeSet<Entry<String, String>>(new Comparator<Entry<String, String>>(){
         public int compare(Entry<String, String> o1, Entry<String, String> o2)
         {
            return o1.getKey().compareTo(o2.getKey());
         }
      });
      for(int i = deleteIndex.length-1; i >= 0; i--){
         keyvals.remove(deleteIndex[i]);
      }
      expectedSet.addAll(keyvals);
      List<Entry<String, String>> expected = new ArrayList<Entry<String, String>>(expectedSet);
      
      StringDbIterator actual = new StringDbIterator(db.iterator());
      actual.seek("f");
      for(int i = expected.size()-1; i >= 0; i--){
         assertTrue(actual.hasPrev());
         assertEquals(expected.get(i), actual.peekPrev());
         assertEquals(expected.get(i), actual.prev());
      }
      assertFalse(actual.hasPrev());

      actual.seek("g");
      assertFalse(actual.hasNext());
      for(int i = expected.size()-1; i >= 0; i--){
         assertTrue(actual.hasPrev());
         assertEquals(expected.get(i), actual.peekPrev());
         assertEquals(expected.get(i), actual.prev());
      }
      assertFalse(actual.hasPrev());
      
      //recreating a strange set of circumstances encountered in the field
      actual.seek("g");
      assertFalse(actual.hasNext());
      actual.seekToLast();
      List<Entry<String, String>> items = new ArrayList<Entry<String, String>>();
      Entry<String, String> peek = actual.peek();
      items.add(peek);
      assertEquals(expected.get(expected.size()-1), peek);
      assertTrue(actual.hasPrev());
      Entry<String, String> prev = actual.prev();
      items.add(prev);
      assertEquals(expected.get(expected.size()-2), prev);
      
      while(actual.hasPrev()){
         items.add(actual.prev());
      }
      
      Collections.reverse(items);
      assertEquals(expected, items);
   }

   private void putAll(DB db, Iterable<Entry<String, String>> entries){
      for(Entry<String, String> e:entries){
         db.put(e.getKey().getBytes(UTF_8), e.getValue().getBytes(UTF_8));
      }
   }

   private boolean hasFollowing(int direction, ReverseIterator<?> iter)
   {
      return direction < 0 ? iter.hasPrev() : iter.hasNext();
   }

   private <T> T peekFollowing(int direction, ReversePeekingIterator<T> iter)
   {
      return direction < 0 ? iter.peekPrev() : iter.peek();
   }

   private <T> T getFollowing(int direction, ReverseIterator<T> iter)
   {
      return direction < 0 ? iter.prev() : iter.next();
   }

   private static class StringDbIterator implements ReverseSeekingIterator<String, String>
   {
      private DBIterator iterator;

      private StringDbIterator(DBIterator iterator)
      {
         this.iterator = iterator;
      }

      @Override
      public boolean hasNext()
      {
         return iterator.hasNext();
      }

      @Override
      public void seekToFirst()
      {
         iterator.seekToFirst();
      }

      @Override
      public void seek(String targetKey)
      {
         iterator.seek(targetKey.getBytes(UTF_8));
      }

      @Override
      public Entry<String, String> peek()
      {
         return adapt(iterator.peekNext());
      }

      @Override
      public Entry<String, String> next()
      {
         return adapt(iterator.next());
      }

      @Override
      public void remove()
      {
         throw new UnsupportedOperationException();
      }

      private Entry<String, String> adapt(Entry<byte[], byte[]> next)
      {
         return Maps.immutableEntry(new String(next.getKey(), UTF_8), new String(next.getValue(),
               UTF_8));
      }

      @Override
      public Entry<String, String> peekPrev()
      {
         return adapt(iterator.peekPrev());
      }

      @Override
      public Entry<String, String> prev()
      {
         return adapt(iterator.prev());
      }

      @Override
      public boolean hasPrev()
      {
         return iterator.hasPrev();
      }

      @Override
      public void seekToLast()
      {
         iterator.seekToLast();
      }
      
      @Override
      public void seekToEnd(){
         // ignore this, it's a complication of the class hierarchy that doesnt need to be fixed for
         // testing as of yet
         throw new UnsupportedOperationException();
      }

      public static class EntryCompare implements Comparator<Entry<String, String>>
      {
         @Override
         public int compare(Entry<String, String> o1, Entry<String, String> o2)
         {
            return o1.getKey().compareTo(o2.getKey());
         }
      }
      public static class ReverseEntryCompare extends EntryCompare
      {
         @Override
         public int compare(Entry<String, String> o1, Entry<String, String> o2)
         {
            return -super.compare(o1, o2);
         }
      }
   }
}
