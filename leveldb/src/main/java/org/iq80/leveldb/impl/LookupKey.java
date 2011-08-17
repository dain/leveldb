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

import org.iq80.leveldb.util.VariableLengthQuantity;
import org.jboss.netty.buffer.ChannelBuffer;
import org.iq80.leveldb.util.Buffers;

import static org.iq80.leveldb.util.SizeOf.SIZE_OF_LONG;

public class LookupKey
{
    // We construct a buffer of the form:
    //    klength  varint32               <-- start_
    //    userkey  char[klength]          <-- kstart_
    //    tag      uint64
    //                                    <-- end_
    // The array is a suitable MemTable key.
    // The suffix starting with "userkey" can be used as an InternalKey.

    private final ChannelBuffer key;
    private final int keyStart;

    public LookupKey(ChannelBuffer user_key, long sequenceNumber)
    {
        // A conservative estimate of the key length
        // todo add function to calculate exact size of packed int
        key = Buffers.buffer(user_key.readableBytes() + 13);

        // write length
        VariableLengthQuantity.packInt(user_key.readableBytes() + 8, key);
        keyStart = key.readableBytes();

        // write bytes
        key.writeBytes(user_key, 0, user_key.readableBytes());

        // write sequence number
        key.writeLong(SequenceNumber.packSequenceAndValueType(sequenceNumber, ValueType.VALUE));
    }

    public ChannelBuffer getMemtableKey()
    {
        // full key
        return key.duplicate();
    }

    public InternalKey getInternalKey()
    {
        // user key + tag
        return new InternalKey(key.slice(keyStart, key.readableBytes() - keyStart));
    }

    public ChannelBuffer getUserKey()
    {
        // just user key part -- no key length and no tag
        return key.slice(keyStart, key.readableBytes() - keyStart - SIZE_OF_LONG);
    }

    @Override
    public String toString()
    {
        return getInternalKey().toString();
    }
}
