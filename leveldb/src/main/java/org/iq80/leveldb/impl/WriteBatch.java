package org.iq80.leveldb.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.jboss.netty.buffer.ChannelBuffer;

import java.util.List;
import java.util.Map.Entry;

import static com.google.common.collect.Lists.newArrayList;

public class WriteBatch
{
    private List<Entry<ChannelBuffer, ChannelBuffer>> batch = newArrayList();
    private int approximateSize;

    public int getApproximateSize()
    {
        return approximateSize;
    }

    public int size()
    {
        return batch.size();
    }

    public WriteBatch put(ChannelBuffer key, ChannelBuffer value)
    {
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(value, "value is null");
        batch.add(Maps.immutableEntry(key, value));
        approximateSize += 12 + key.readableBytes() + value.readableBytes();
        return this;
    }

    public WriteBatch delete(ChannelBuffer key)
    {
        Preconditions.checkNotNull(key, "key is null");
        batch.add(Maps.immutableEntry(key, (ChannelBuffer) null));
        approximateSize += 6 + key.readableBytes();
        return this;
    }

    public void forEach(Handler handler) {
        for (Entry<ChannelBuffer, ChannelBuffer> entry : batch) {
            ChannelBuffer key = entry.getKey();
            ChannelBuffer value = entry.getValue();
            if (value != null) {
                handler.put(key, value);
            } else {
                handler.delete(key);
            }
        }
    }

    public static interface  Handler {

        void put(ChannelBuffer key, ChannelBuffer value);

        void delete(ChannelBuffer key);
    }

}
