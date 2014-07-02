
package org.iq80.leveldb.util;
import java.util.Arrays;

import org.iq80.leveldb.impl.ReverseIterators;
import org.iq80.leveldb.impl.ReversePeekingIterator;

import junit.framework.TestCase;

public class ReversePeekingIteratorTest extends TestCase
{
   public void testNextPrevPeekPeekPrev(){
      Integer a = 0, b = 1, c = 2, d = 3;
      ReversePeekingIterator<Integer> iter = ReverseIterators.reversePeekingIterator(Arrays.asList(a, b, c, d));
      
      assertTrue(iter.hasNext());
      assertFalse(iter.hasPrev());
      assertEquals(a, iter.peek());
      
      //make sure the peek did not advance anything
      assertTrue(iter.hasNext());
      assertFalse(iter.hasPrev());
      assertEquals(a, iter.peek());

      assertEquals(a, iter.next());
      assertTrue(iter.hasNext());
      assertTrue(iter.hasPrev());
      assertEquals(a, iter.peekPrev());

      assertTrue(iter.hasNext());
      assertTrue(iter.hasPrev());
      assertEquals(b, iter.peek());
      assertEquals(b, iter.next());

      assertTrue(iter.hasNext());
      assertTrue(iter.hasPrev());
      assertEquals(b, iter.peekPrev());
      assertEquals(c, iter.peek());
      
      assertEquals(b, iter.prev());
      assertEquals(b, iter.peek());
      assertEquals(a, iter.peekPrev());

      assertEquals(b, iter.next());
      assertEquals(c, iter.next());
      assertEquals(d, iter.next());
      assertFalse(iter.hasNext());
      assertTrue(iter.hasPrev());
   }

}


