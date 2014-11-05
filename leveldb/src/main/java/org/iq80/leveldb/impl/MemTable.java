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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.iq80.leveldb.util.InternalIterator;
import org.iq80.leveldb.util.Slice;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.iq80.leveldb.util.SizeOf.SIZE_OF_LONG;

public class MemTable
        implements SeekingIterable<InternalKey, Slice>
{
    private final ConcurrentSkipListMap<InternalKey, Slice> table;
    private final AtomicLong approximateMemoryUsage = new AtomicLong();

    public MemTable(InternalKeyComparator internalKeyComparator)
    {
        table = new ConcurrentSkipListMap<InternalKey, Slice>(internalKeyComparator);
    }

    public boolean isEmpty()
    {
        return table.isEmpty();
    }

    public long approximateMemoryUsage()
    {
        return approximateMemoryUsage.get();
    }

    public void add(long sequenceNumber, ValueType valueType, Slice key, Slice value)
    {
        Preconditions.checkNotNull(valueType, "valueType is null");
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(valueType, "valueType is null");

        InternalKey internalKey = new InternalKey(key, sequenceNumber, valueType);
        table.put(internalKey, value);

        approximateMemoryUsage.addAndGet(key.length() + SIZE_OF_LONG + value.length());
    }

    public LookupResult get(LookupKey key)
    {
        Preconditions.checkNotNull(key, "key is null");

        InternalKey internalKey = key.getInternalKey();
        Entry<InternalKey, Slice> entry = table.ceilingEntry(internalKey);
        if (entry == null) {
            return null;
        }

        InternalKey entryKey = entry.getKey();
        if (entryKey.getUserKey().equals(key.getUserKey())) {
            if (entryKey.getValueType() == ValueType.DELETION) {
                return LookupResult.deleted(key);
            }
            else {
                return LookupResult.ok(key, entry.getValue());
            }
        }
        return null;
    }

    @Override
    public MemTableIterator iterator()
    {
        return new MemTableIterator();
    }

    public class MemTableIterator
            implements
            InternalIterator,
            ReverseSeekingIterator<InternalKey, Slice>
    {

        private ReversePeekingIterator<Entry<InternalKey, Slice>> iterator;
        private final List<Entry<InternalKey, Slice>> entryList;

        public MemTableIterator()
        {
            entryList = Lists.newArrayList(table.entrySet());
            seekToFirst();
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public void seekToFirst()
        {
            makeIteratorAtIndex(0);
        }

        @Override
        public void seek(InternalKey targetKey)
        {
            int index = Collections.binarySearch(entryList, Maps.immutableEntry(targetKey, (Slice) null), new Comparator<Entry<InternalKey, Slice>>()
            {
                public int compare(Entry<InternalKey, Slice> o1, Entry<InternalKey, Slice> o2)
                {
                    return table.comparator().compare(o1.getKey(), o2.getKey());
                }
            });
            if (index < 0) {
            /*
             * from Collections.binarySearch:
             * index: the index of the search key, if it is contained in the list; otherwise, (-(insertion point) - 1).
             * The insertion point is defined as the point at which the key would be inserted into the list:
             * the index of the first element greater than the key, or list.size() if all elements in the list are
             * less than the specified key
             */
                index = -(index + 1);
            }
            makeIteratorAtIndex(index);
        }

        @Override
        public Entry<InternalKey, Slice> peek()
        {
            return iterator.peek();
        }

        @Override
        public Entry<InternalKey, Slice> next()
        {
            return iterator.next();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void seekToLast()
        {
            if (entryList.size() == 0) {
                seekToFirst();
                return;
            }
            makeIteratorAtIndex(this.entryList.size() - 1);
        }

        @Override
        public void seekToEnd()
        {
            makeIteratorAtIndex(this.entryList.size());
        }

        private void makeIteratorAtIndex(int index)
        {
            iterator = ReverseIterators.reversePeekingIterator(this.entryList.listIterator(index));
        }

        @Override
        public Entry<InternalKey, Slice> peekPrev()
        {
            return iterator.peekPrev();
        }

        @Override
        public Entry<InternalKey, Slice> prev()
        {
            return iterator.prev();
        }

        @Override
        public boolean hasPrev()
        {
            return iterator.hasPrev();
        }
    }
}
