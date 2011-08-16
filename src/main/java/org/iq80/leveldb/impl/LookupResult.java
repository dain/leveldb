package org.iq80.leveldb.impl;

import com.google.common.base.Preconditions;
import org.jboss.netty.buffer.ChannelBuffer;
import org.iq80.leveldb.util.Buffers;

public class LookupResult
{
    public static LookupResult ok(LookupKey key, ChannelBuffer value)
    {
        return new LookupResult(key, value, false);
    }

    public static LookupResult deleted(LookupKey key)
    {
        return new LookupResult(key, null, true);
    }

    private final LookupKey key;
    private final ChannelBuffer value;
    private final boolean deleted;

    private LookupResult(LookupKey key, ChannelBuffer value, boolean deleted)
    {
        Preconditions.checkNotNull(key, "key is null");
        this.key = key;
        if (value != null) {
            this.value = Buffers.unmodifiableBuffer(value.slice());
        } else {
            this.value = null;
        }
        this.deleted = deleted;
    }

    public LookupKey getKey()
    {
        return key;
    }

    public ChannelBuffer getValue()
    {
        if (value == null) {
            return null;
        }
        return value.duplicate();
    }

    public boolean isDeleted()
    {
        return deleted;
    }
}
