package org.iq80.leveldb.util;

import org.iq80.leveldb.impl.SeekingIterator;

import java.util.Map.Entry;

public final class ConcatenatingIterator<K, V> extends AbstractSeekingIterator<K, V>
{
    private final SeekingIterator<K, ? extends SeekingIterator<K, ? extends V>> inputs;
    private SeekingIterator<K, ? extends V> current;

    public ConcatenatingIterator(SeekingIterator<K, ? extends SeekingIterator<K, ? extends V>> inputs)
    {
        this.inputs = inputs;
        current = SeekingIterators.emptyIterator();
    }

    @Override
    protected void seekToFirstInternal()
    {
        // reset index to before first and clear the data iterator
        inputs.seekToFirst();
        current = SeekingIterators.emptyIterator();
    }

    @Override
    protected void seekInternal(K targetKey)
    {
        // seek the index to the block containing the key
        inputs.seek(targetKey);

        // if indexIterator does not have a next, it mean the key does not exist in this iterator
        if (inputs.hasNext()) {
            // seek the current iterator to the key
            current = inputs.next().getValue();
            current.seek(targetKey);
        }
        else {
            current = SeekingIterators.emptyIterator();
        }
    }

    @Override
    protected Entry<K, V> getNextElement()
    {
        // note: it must be here & not where 'current' is assigned,
        // because otherwise we'll have called inputs.next() before throwing
        // the first NPE, and the next time around we'll call inputs.next()
        // again, incorrectly moving beyond the error.
        boolean currentHasNext;
        while (!(currentHasNext = current.hasNext()) && inputs.hasNext()) {
            current = inputs.next().getValue();
        }
        if (currentHasNext) {
            return (Entry<K, V>) current.next();
        }
        else {
            // set current to empty iterator to avoid extra calls to user iterators
            current = SeekingIterators.emptyIterator();
            return null;
        }
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("ConcatenatingIterator");
        sb.append("{inputs=").append(inputs);
        sb.append(", current=").append(current);
        sb.append('}');
        return sb.toString();
    }
}
