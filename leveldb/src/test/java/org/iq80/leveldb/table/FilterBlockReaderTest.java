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

import org.iq80.leveldb.util.DynamicSliceOutput;
import org.iq80.leveldb.util.Hash;
import org.iq80.leveldb.util.Slice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertTrue;

/**
 * @author Honore Vasconcelos
 */
public class FilterBlockReaderTest
{
    {
        final FilterBlockBuilder filterBlockBuilder = new FilterBlockBuilder(new BloomFilterPolicy(10));
        filterBlockBuilder.startBlock(189);
        for (int i = 0; i < 2000; ++i) {
            filterBlockBuilder.addKey(new Slice(String.format("key%06d", i).getBytes()));
        }
        final Slice finish = filterBlockBuilder.finish();
        final FilterBlockReader reader = new FilterBlockReader(new BloomFilterPolicy(10), finish);
        for (int i = 0; i < 2000; ++i) {
            assertTrue(reader.keyMayMatch(189, new Slice(String.format("key%06d", i).getBytes())));
        }
    }

    private static class TestHashFilter implements FilterPolicy
    {
        @Override
        public String name()
        {
            return "TestHashFilter";
        }

        @Override
        public byte[] createFilter(List<Slice> keys)
        {
            final DynamicSliceOutput out = new DynamicSliceOutput(100);
            for (Slice key : keys) {
                out.writeInt(Hash.hash(key.getRawArray(), key.getRawOffset(), key.length(), 1));
            }
            return out.slice().copyBytes();
        }

        @Override
        public boolean keyMayMatch(Slice key, Slice filter)
        {
            final int hash = Hash.hash(key.getRawArray(), key.getRawOffset(), key.length(), 1);
            for (int i = 0; i + 4 <= filter.length(); i += 4) {
                if (hash == filter.getInt(i)) {
                    return true;
                }
            }
            return false;
        }
    }

    @Test
    public void testEmptyBuilder() throws Exception
    {
        FilterBlockBuilder builder = new FilterBlockBuilder(new TestHashFilter());
        final Slice finish = builder.finish();
        assertTrue(Arrays.equals(finish.copyBytes(), new byte[]{0, 0, 0, 0, 11}));
        final FilterBlockReader reader = new FilterBlockReader(new TestHashFilter(), finish);
        assertTrue(reader.keyMayMatch(0, new Slice("foo".getBytes())));
        assertTrue(reader.keyMayMatch(100000, new Slice("foo".getBytes())));
    }

    @Test
    public void testSingleChunk() throws IOException
    {
        FilterBlockBuilder builder = new FilterBlockBuilder(new TestHashFilter());
        builder.startBlock(100);
        builder.addKey(new Slice("foo".getBytes()));
        builder.addKey(new Slice("bar".getBytes()));
        builder.addKey(new Slice("box".getBytes()));
        builder.startBlock(200);
        builder.addKey(new Slice("box".getBytes()));
        builder.startBlock(300);
        builder.addKey(new Slice("hello".getBytes()));
        Slice block = builder.finish();
        final FilterBlockReader reader = new FilterBlockReader(new TestHashFilter(), block);
        assertTrue(reader.keyMayMatch(100, new Slice("foo".getBytes())));
        assertTrue(reader.keyMayMatch(100, new Slice("bar".getBytes())));
        assertTrue(reader.keyMayMatch(100, new Slice("box".getBytes())));
        assertTrue(reader.keyMayMatch(100, new Slice("hello".getBytes())));
        assertTrue(reader.keyMayMatch(100, new Slice("foo".getBytes())));
        assertTrue(!reader.keyMayMatch(100, new Slice("missing".getBytes())));
        assertTrue(!reader.keyMayMatch(100, new Slice("other".getBytes())));
    }

    @Test
    public void testMultiChunk()
    {
        FilterBlockBuilder builder = new FilterBlockBuilder(new TestHashFilter());

        // First filter
        builder.startBlock(0);
        builder.addKey(new Slice("foo".getBytes()));
        builder.startBlock(2000);
        builder.addKey(new Slice("bar".getBytes()));

        // Second filter
        builder.startBlock(3100);
        builder.addKey(new Slice("box".getBytes()));

        // Third filter is empty

        // Last filter
        builder.startBlock(9000);
        builder.addKey(new Slice("box".getBytes()));
        builder.addKey(new Slice("hello".getBytes()));

        Slice block = builder.finish();
        final FilterBlockReader reader = new FilterBlockReader(new TestHashFilter(), block);

        // Check first filter
        assertTrue(reader.keyMayMatch(0, new Slice("foo".getBytes())));
        assertTrue(reader.keyMayMatch(2000, new Slice("bar".getBytes())));
        assertTrue(!reader.keyMayMatch(0, new Slice("box".getBytes())));
        assertTrue(!reader.keyMayMatch(0, new Slice("hello".getBytes())));

        // Check second filter
        assertTrue(reader.keyMayMatch(3100, new Slice("box".getBytes())));
        assertTrue(!reader.keyMayMatch(3100, new Slice("foo".getBytes())));
        assertTrue(!reader.keyMayMatch(3100, new Slice("bar".getBytes())));
        assertTrue(!reader.keyMayMatch(3100, new Slice("hello".getBytes())));

        // Check third filter (empty)
        assertTrue(!reader.keyMayMatch(4100, new Slice("foo".getBytes())));
        assertTrue(!reader.keyMayMatch(4100, new Slice("bar".getBytes())));
        assertTrue(!reader.keyMayMatch(4100, new Slice("box".getBytes())));
        assertTrue(!reader.keyMayMatch(4100, new Slice("hello".getBytes())));

        // Check last filter
        assertTrue(reader.keyMayMatch(9000, new Slice("box".getBytes())));
        assertTrue(reader.keyMayMatch(9000, new Slice("hello".getBytes())));
        assertTrue(!reader.keyMayMatch(9000, new Slice("foo".getBytes())));
        assertTrue(!reader.keyMayMatch(9000, new Slice("bar".getBytes())));
    }
}
