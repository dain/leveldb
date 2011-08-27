package org.iq80.leveldb.util;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import org.iq80.leveldb.impl.SeekingIterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public final class MergingIterator<K, V> extends AbstractSeekingIterator<K, V>
{
    private final Iterable<? extends SeekingIterator<K, ? extends V>> inputs;
    private final PriorityQueue<ComparableIterator<K, V>> priorityQueue;
    private final Comparator<K> comparator;

    public MergingIterator(Iterable<? extends SeekingIterator<K, ? extends V>> inputs, Comparator<K> comparator)
    {
        this.inputs = inputs;
        this.comparator = comparator;

        this.priorityQueue = new PriorityQueue<ComparableIterator<K, V>>(Iterables.size(inputs));
        resetPriorityQueue(inputs, comparator);
    }

    @Override
    protected void seekToFirstInternal()
    {
        for (SeekingIterator<K, ? extends V> input : inputs) {
            input.seekToFirst();
        }
        resetPriorityQueue(inputs, comparator);
    }

    @Override
    protected void seekInternal(K targetKey)
    {
        for (SeekingIterator<K, ? extends V> input : inputs) {
            input.seek(targetKey);
        }
        resetPriorityQueue(inputs, comparator);
    }

    private void resetPriorityQueue(Iterable<? extends SeekingIterator<K, ? extends V>> inputs, Comparator<K> comparator)
    {
        int i = 0;
        for (SeekingIterator<K, ? extends V> input : inputs) {
            if (input.hasNext()) {
                priorityQueue.add(new ComparableIterator<K, V>(input, comparator, i++, (Entry<K, V>) input.next()));
            }
        }
    }

    @Override
    protected Entry<K, V> getNextElement()
    {
        Entry<K, V> result = null;
        ComparableIterator<K, V> nextIterator = priorityQueue.poll();
        if (nextIterator != null) {
            result = nextIterator.next();
            if (nextIterator.hasNext()) {
                priorityQueue.add(nextIterator);
            }
        }
        return result;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("MergingIterator");
        sb.append("{inputs=").append(Iterables.toString(inputs));
        sb.append(", comparator=").append(comparator);
        sb.append('}');
        return sb.toString();
    }

    private static class ComparableIterator<K, V> implements Iterator<Entry<K, V>>, Comparable<ComparableIterator<K, V>> {
        private final SeekingIterator<K, ? extends V> iterator;
        private final Comparator<K> comparator;
        private final int ordinal;
        private Entry<K,V> nextElement;

        private ComparableIterator(SeekingIterator<K, ? extends V> iterator, Comparator<K> comparator, int ordinal, Entry<K, V> nextElement)
        {
            this.iterator = iterator;
            this.comparator = comparator;
            this.ordinal = ordinal;
            this.nextElement = nextElement;
        }

        @Override
        public boolean hasNext()
        {
            return nextElement != null;
        }

        public Entry<K, V> next()
        {
            if (nextElement == null) {
                throw new NoSuchElementException();
            }

            Entry<K, V> result = nextElement;
            if (iterator.hasNext()) {
                nextElement = (Entry<K, V>) iterator.next();
            } else {
                nextElement = null;
            }
            return result;
        }

        @Override
        public void remove()
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

            ComparableIterator<?, ?> comparableIterator = (ComparableIterator<?, ?>) o;

            if (ordinal != comparableIterator.ordinal) {
                return false;
            }
            if (nextElement != null ? !nextElement.equals(comparableIterator.nextElement) : comparableIterator.nextElement != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = ordinal;
            result = 31 * result + (nextElement != null ? nextElement.hashCode() : 0);
            return result;
        }

        @Override
        public int compareTo(ComparableIterator<K, V> that)
        {
            int result = comparator.compare(this.nextElement.getKey(), that.nextElement.getKey());
            if (result == 0) {
                result = Ints.compare(this.ordinal, that.ordinal);
            }
            return result;
        }
    }
}
