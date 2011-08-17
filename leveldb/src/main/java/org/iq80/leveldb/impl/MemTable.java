/**
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
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.iq80.leveldb.SeekingIterable;
import org.iq80.leveldb.SeekingIterator;
import org.jboss.netty.buffer.ChannelBuffer;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.iq80.leveldb.util.SizeOf.SIZE_OF_LONG;

public class MemTable implements SeekingIterable<InternalKey, ChannelBuffer>
{
    private final ConcurrentSkipListMap<InternalKey, ChannelBuffer> table;
    private AtomicLong approximateMemoryUsage = new AtomicLong();

    public MemTable(InternalKeyComparator internalKeyComparator)
    {
        table = new ConcurrentSkipListMap<InternalKey, ChannelBuffer>(internalKeyComparator);
    }

    public boolean isEmpty()
    {
        return table.isEmpty();
    }

    public long approximateMemoryUsage()
    {
        return approximateMemoryUsage.get();
    }

    public void add(long sequenceNumber, ValueType valueType, ChannelBuffer key, ChannelBuffer value)
    {
        Preconditions.checkNotNull(valueType, "valueType is null");
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(valueType, "valueType is null");

        InternalKey internalKey = new InternalKey(key, sequenceNumber, valueType);
        table.put(internalKey, value);

        approximateMemoryUsage.addAndGet(key.readableBytes() + SIZE_OF_LONG + value.readableBytes());
    }

    public LookupResult get(LookupKey key)
    {
        Preconditions.checkNotNull(key, "key is null");

        InternalKey internalKey = key.getInternalKey();
        Entry<InternalKey,ChannelBuffer> entry = table.ceilingEntry(internalKey);
        if (entry == null) {
            return null;
        }

        if (entry.getKey().getValueType() == ValueType.DELETION) {
            return LookupResult.deleted(key);
        }

        if (entry.getKey().getUserKey().equals(key.getUserKey())) {
            return LookupResult.ok(key, entry.getValue());
        }
        return null;
    }

    @Override
    public MemTableIterator iterator()
    {
        return new MemTableIterator();
    }

    public class MemTableIterator implements SeekingIterator<InternalKey, ChannelBuffer>
    {

        private PeekingIterator<Entry<InternalKey,ChannelBuffer>> iterator;

        public MemTableIterator()
        {
            iterator = Iterators.peekingIterator(table.entrySet().iterator());
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public void seekToFirst()
        {
            iterator = Iterators.peekingIterator(table.entrySet().iterator());
        }

        @Override
        public void seek(InternalKey targetKey)
        {
            iterator = Iterators.peekingIterator(table.tailMap(targetKey).entrySet().iterator());
        }

        @Override
        public InternalEntry peek()
        {
            Entry<InternalKey, ChannelBuffer> entry = iterator.peek();
            return new InternalEntry(entry.getKey(), entry.getValue());
        }

        @Override
        public InternalEntry next()
        {
            Entry<InternalKey, ChannelBuffer> entry = iterator.next();
            return new InternalEntry(entry.getKey(), entry.getValue());
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }
}
