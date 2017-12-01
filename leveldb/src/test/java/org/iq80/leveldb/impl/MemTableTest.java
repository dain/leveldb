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
package org.iq80.leveldb.impl;

import org.iq80.leveldb.table.BytewiseComparator;
import org.testng.annotations.Test;

import static org.iq80.leveldb.util.TestUtils.asciiToBytes;
import static org.iq80.leveldb.util.TestUtils.asciiToSlice;
import static org.testng.Assert.assertEquals;

public class MemTableTest
{
    @Test
    public void testTestSimple() throws Exception
    {
        InternalKeyComparator cmp = new InternalKeyComparator(new BytewiseComparator());
        final MemTable memtable = new MemTable(cmp);
        WriteBatchImpl batch = new WriteBatchImpl();
        batch.put(asciiToBytes("k1"), asciiToBytes("v1"));
        batch.put(asciiToBytes("k1"), asciiToBytes("v1"));
        batch.put(asciiToBytes("k2"), asciiToBytes("v2"));
        batch.put(asciiToBytes("k3"), asciiToBytes("v3"));
        batch.put(asciiToBytes("largekey"), asciiToBytes("vlarge"));
        batch.forEach(new InsertIntoHandler(memtable, 100));
        final MemTable.MemTableIterator iter = memtable.iterator();
        iter.seekToFirst();
        assertEquals(new InternalEntry(new InternalKey(asciiToSlice("k1"), 101, ValueType.VALUE), asciiToSlice("v1")), iter.next());
        assertEquals(new InternalEntry(new InternalKey(asciiToSlice("k1"), 100, ValueType.VALUE), asciiToSlice("v1")), iter.next());
        assertEquals(new InternalEntry(new InternalKey(asciiToSlice("k2"), 102, ValueType.VALUE), asciiToSlice("v2")), iter.next());
        assertEquals(new InternalEntry(new InternalKey(asciiToSlice("k3"), 103, ValueType.VALUE), asciiToSlice("v3")), iter.next());
        assertEquals(new InternalEntry(new InternalKey(asciiToSlice("largekey"), 104, ValueType.VALUE), asciiToSlice("vlarge")), iter.next());
    }
}
