package org.iq80.leveldb.impl;

import com.google.common.base.Preconditions;
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
    public DbEntry next()
    {
        return adapt(seekingIterator.next());
    }

    @Override
    public DbEntry peekNext()
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

    private DbEntry adapt(Entry<Slice, Slice> entry)
    {
        return new DbEntry(entry.getKey(), entry.getValue());
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
    public DbEntry prev()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public DbEntry peekPrev()
    {
        throw new UnsupportedOperationException();
    }

    public static class DbEntry implements Entry<byte[], byte[]>
    {
        private final Slice key;
        private final Slice value;

        public DbEntry(Slice key, Slice value)
        {
            Preconditions.checkNotNull(key, "key is null");
            Preconditions.checkNotNull(value, "value is null");
            this.key = key;
            this.value = value;
        }

        @Override
        public byte[] getKey()
        {
            return key.getBytes();
        }

        public Slice getKeySlice()
        {
            return key;
        }

        @Override
        public byte[] getValue()
        {
            return value.getBytes();
        }

        public Slice getValueSlice()
        {
            return value;
        }

        @Override
        public byte[] setValue(byte[] value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object object)
        {
            if (object instanceof Entry) {
                Entry<?, ?> that = (Entry<?, ?>) object;
                return key.equals(that.getKey()) &&
                        value.equals(that.getValue());
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            return key.hashCode() ^ value.hashCode();
        }

        /**
         * Returns a string representation of the form <code>{key}={value}</code>.
         */
        @Override
        public String toString()
        {
            return key + "=" + value;
        }
    }
}
