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

import com.google.common.collect.Maps;
import org.iq80.leveldb.util.AbstractSeekingIterator;
import org.iq80.leveldb.util.DbIterator;
import org.iq80.leveldb.util.Slice;

import java.util.Comparator;
import java.util.Map.Entry;

public final class SnapshotSeekingIterator
        extends AbstractSeekingIterator<Slice, Slice> implements AutoCloseable
{
    private final DbIterator iterator;
    private final long sequence;
    private final Comparator<Slice> userComparator;

    public SnapshotSeekingIterator(DbIterator iterator, long sequence, Comparator<Slice> userComparator)
    {
        this.iterator = iterator;
        this.sequence = sequence;
        this.userComparator = userComparator;
    }

    @Override
    public void close()
    {
        next = null;
        iterator.close();
    }

    @Override
    protected void seekToFirstInternal()
    {
        next = null;
        iterator.seekToFirst();
        findNextUserEntry();
    }

    @Override
    protected void seekInternal(Slice targetKey)
    {
        next = null;
        iterator.seek(new InternalKey(targetKey, sequence, ValueType.VALUE));
        findNextUserEntry();
    }

    @Override
    protected Entry<Slice, Slice> getNextElement()
    {
        if (this.next == null && !iterator.hasNext()) {
            return null;
        }
        // find the next user entry after the key we are about to return
        findNextUserEntry();
        if (next != null) {
            Entry<InternalKey, Slice> next = this.next;
            this.next = null;
            return Maps.immutableEntry(next.getKey().getUserKey(), next.getValue());
        }
        return null;
    }

    Entry<InternalKey, Slice> next;

    private void findNextUserEntry()
    {
        if (next != null) {
            return;
        }
        // if there are no more entries, we are done
        if (!iterator.hasNext()) {
            return;
        }
        //todo optimize algorithm. we should not do early load when called from #seekX(y)
        while (iterator.hasNext()) {
            Entry<InternalKey, Slice> next = iterator.next();
            InternalKey key = next.getKey();
            // skip entries created after our snapshot
            if (key.getSequenceNumber() > sequence) {
                continue;
            }
            if (key.getValueType() == ValueType.DELETION) {
                while (iterator.hasNext()) {
                    Entry<InternalKey, Slice> peek = iterator.peek();
                    if (peek.getKey().getValueType() == ValueType.DELETION) {
                        break; //handled by next loop
                    }
                    else if (peek.getKey().getValueType() == ValueType.VALUE && userComparator.compare(key.getUserKey(), peek.getKey().getUserKey()) == 0) {
                        iterator.next(); // Entry hidden
                    }
                    else {
                        break; //different key
                    }
                }
            }
            else if (key.getValueType() == ValueType.VALUE) {
                while (iterator.hasNext()) {
                    Entry<InternalKey, Slice> peek = iterator.peek();
                    if (peek.getKey().getValueType() == ValueType.VALUE && userComparator.compare(key.getUserKey(), peek.getKey().getUserKey()) == 0) {
                        iterator.next(); // Entry hidden
                    }
                    else {
                        this.next = next;
                        return;
                    }
                }
                this.next = next;
                return;
            }
        }
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("SnapshotSeekingIterator");
        sb.append("{sequence=").append(sequence);
        sb.append(", iterator=").append(iterator);
        sb.append('}');
        return sb.toString();
    }
}
