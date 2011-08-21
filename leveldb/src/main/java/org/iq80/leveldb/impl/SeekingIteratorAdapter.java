package org.iq80.leveldb.impl;

import com.google.common.collect.Maps;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.Slices;

import java.util.Map.Entry;

public class SeekingIteratorAdapter implements DBIterator
{
    private final SeekingIterator<Slice, Slice> seekingIterator;

    public SeekingIteratorAdapter(SeekingIterator<Slice, Slice> seekingIterator)
    {
        this.seekingIterator = seekingIterator;
    }

    @Override
    public void seekToFirst()
    {
        seekingIterator.seekToFirst();
    }

    @Override
    public void seek(byte[] targetKey)
    {
        seekingIterator.seek(Slices.wrappedBuffer(targetKey));
    }

    @Override
    public boolean hasNext()
    {
        return seekingIterator.hasNext();
    }

    @Override
    public Entry<byte[], byte[]> next()
    {
        return adapt(seekingIterator.next());
    }

    @Override
    public Entry<byte[], byte[]> peekNext()
    {
        return adapt(seekingIterator.peek());
    }

    @Override
    public void close()
    {
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    private Entry<byte[], byte[]> adapt(Entry<Slice, Slice> entry)
    {
        return Maps.immutableEntry(entry.getKey().getBytes(), entry.getValue().getBytes());
    }

    //
    // todo Implement reverse iterator
    //


    @Override
    public void seekToLast()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasPrev()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Entry<byte[], byte[]> prev()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Entry<byte[], byte[]> peekPrev()
    {
        throw new UnsupportedOperationException();
    }
}
