package org.iq80.leveldb.util;

import com.google.common.primitives.Ints;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.SeekingIterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public final class VersionIterator extends AbstractSeekingIterator<InternalKey, Slice>
{
    private final Level0Iterator level0;
    private final List<LevelIterator> levels;
    private final PriorityQueue<ComparableIterator> priorityQueue;
    private final Comparator<InternalKey> comparator;

    public VersionIterator(Level0Iterator level0, List<LevelIterator> levels, Comparator<InternalKey> comparator)
    {
        this.level0 = level0;
        this.levels = levels;
        this.comparator = comparator;

        this.priorityQueue = new PriorityQueue<ComparableIterator>(levels.size() + 1);
        resetPriorityQueue(comparator);
    }

    @Override
    protected void seekToFirstInternal()
    {
        if (level0 != null) {
            level0.seekToFirst();
        }
        for (LevelIterator level : levels) {
            level.seekToFirst();
        }
        resetPriorityQueue(comparator);
    }

    @Override
    protected void seekInternal(InternalKey targetKey)
    {
        if (level0 != null) {
            level0.seekInternal(targetKey);
        }
        for (LevelIterator level : levels) {
            level.seek(targetKey);
        }
        resetPriorityQueue(comparator);
    }

    private void resetPriorityQueue(Comparator<InternalKey> comparator)
    {
        if (level0 != null && level0.hasNext()) {
            priorityQueue.add(new ComparableIterator(level0, comparator, 0, level0.next()));
        }

        int i = 1;
        for (LevelIterator level : levels) {
            if (level.hasNext()) {
                priorityQueue.add(new ComparableIterator(level, comparator, i++, level.next()));
            }
        }
    }

    @Override
    protected Entry<InternalKey, Slice> getNextElement()
    {
        Entry<InternalKey, Slice> result = null;
        ComparableIterator nextIterator = priorityQueue.poll();
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
        sb.append("VersionIterator");
        sb.append("{level0=").append(level0);
        sb.append(", levels=").append(levels);
        sb.append(", comparator=").append(comparator);
        sb.append('}');
        return sb.toString();
    }

    private static class ComparableIterator implements Iterator<Entry<InternalKey, Slice>>, Comparable<ComparableIterator>
    {
        private final SeekingIterator<InternalKey, Slice> iterator;
        private final Comparator<InternalKey> comparator;
        private final int ordinal;
        private Entry<InternalKey, Slice> nextElement;

        private ComparableIterator(SeekingIterator<InternalKey, Slice> iterator, Comparator<InternalKey> comparator, int ordinal, Entry<InternalKey, Slice> nextElement)
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

        public Entry<InternalKey, Slice> next()
        {
            if (nextElement == null) {
                throw new NoSuchElementException();
            }

            Entry<InternalKey, Slice> result = nextElement;
            if (iterator.hasNext()) {
                nextElement = iterator.next();
            }
            else {
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

            ComparableIterator comparableIterator = (ComparableIterator) o;

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
        public int compareTo(ComparableIterator that)
        {
            int result = comparator.compare(this.nextElement.getKey(), that.nextElement.getKey());
            if (result == 0) {
                result = Ints.compare(this.ordinal, that.ordinal);
            }
            return result;
        }
    }
}
