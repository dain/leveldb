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

package org.iq80.leveldb.util;

import java.util.Arrays;

import org.iq80.leveldb.impl.ReverseIterators;
import org.iq80.leveldb.impl.ReversePeekingIterator;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertEquals;

public class ReversePeekingIteratorTest
{
    public void testNextPrevPeekPeekPrev()
    {
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


