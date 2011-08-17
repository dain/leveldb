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
import org.iq80.leveldb.table.UserComparator;
import org.jboss.netty.buffer.ChannelBuffer;
import org.iq80.leveldb.util.Buffers;

import static org.iq80.leveldb.impl.SequenceNumber.MAX_SEQUENCE_NUMBER;

public class InternalUserComparator implements UserComparator
{
    private final InternalKeyComparator internalKeyComparator;

    public InternalUserComparator(InternalKeyComparator internalKeyComparator)
    {
        this.internalKeyComparator = internalKeyComparator;
    }

    @Override
    public int compare(ChannelBuffer left, ChannelBuffer right)
    {
        return internalKeyComparator.compare(new InternalKey(left), new InternalKey(right));
    }

    @Override
    public void findShortestSeparator(
            ChannelBuffer start,
            ChannelBuffer limit)
    {
        // Attempt to shorten the user portion of the key
        ChannelBuffer startUserKey = new InternalKey(start).getUserKey();
        ChannelBuffer limitUserKey = new InternalKey(limit).getUserKey();

        ChannelBuffer tmp = Buffers.directBuffer(startUserKey.readableBytes());
        tmp.writeBytes(startUserKey.duplicate());

        internalKeyComparator.getUserComparator().findShortestSeparator(tmp, limitUserKey);

        if (internalKeyComparator.getUserComparator().compare(startUserKey, tmp) < 0) {
            // User key has become larger.  Tack on the earliest possible
            // number to the shortened user key.
            InternalKey newInternalKey = new InternalKey(tmp, MAX_SEQUENCE_NUMBER, ValueType.VALUE);
            Preconditions.checkState(compare(start, newInternalKey.encode()) < 0);// todo
            Preconditions.checkState(compare(newInternalKey.encode(), limit) < 0);// todo

            // todo it is assumed that start is a dynamic channel buffer and temp space
            start.clear();
            start.writeBytes(newInternalKey.encode());
        }
    }

    @Override
    public void findShortSuccessor(ChannelBuffer key)
    {
        ChannelBuffer userKey = new InternalKey(key).getUserKey();

        ChannelBuffer tmp = Buffers.directBuffer(userKey.readableBytes());
        tmp.writeBytes(userKey.duplicate());

        internalKeyComparator.getUserComparator().findShortSuccessor(tmp.slice());

        if (internalKeyComparator.getUserComparator().compare(userKey, tmp) < 0) {
            // User key has become larger.  Tack on the earliest possible
            // number to the shortened user key.
            InternalKey newInternalKey = new InternalKey(tmp, MAX_SEQUENCE_NUMBER, ValueType.VALUE);
            Preconditions.checkState(compare(key, newInternalKey.encode()) < 0);// todo

            // todo it is assumed that key is a dynamic channel buffer and temp space
            key.clear();
            key.writeBytes(newInternalKey.encode());
        }
    }
}
