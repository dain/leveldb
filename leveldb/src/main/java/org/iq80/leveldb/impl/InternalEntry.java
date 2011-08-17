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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.jboss.netty.buffer.ChannelBuffer;
import org.iq80.leveldb.util.Buffers;

import java.util.Map.Entry;

import static com.google.common.base.Charsets.UTF_8;

public class InternalEntry implements Entry<InternalKey, ChannelBuffer>
{
    public static final Function<InternalEntry, InternalKey> GET_KEY = new Function<InternalEntry, InternalKey>()
    {
        @Override
        public InternalKey apply(InternalEntry internalEntry)
        {
            return internalEntry.getKey();
        }
    };

    private final InternalKey key;
    private final ChannelBuffer value;

    public InternalEntry(InternalKey key, ChannelBuffer value)
    {
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(value, "value is null");
        this.key = key;
        this.value = Buffers.unmodifiableBuffer(value);
    }

    @Override
    public InternalKey getKey()
    {
        return key;
    }

    @Override
    public ChannelBuffer getValue()
    {
        return value.duplicate();
    }


    /**
     * @throws UnsupportedOperationException always
     */
    @Override
    public final ChannelBuffer setValue(ChannelBuffer value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InternalEntry entry = (InternalEntry) o;

        if (!key.equals(entry.key)) {
            return false;
        }
        if (!value.equals(entry.value)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = key.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("InternalEntry");
        sb.append("{key=").append(key);      // todo don't print the real value
        sb.append(", value=").append(value.toString(UTF_8));
        sb.append('}');
        return sb.toString();
    }
}

