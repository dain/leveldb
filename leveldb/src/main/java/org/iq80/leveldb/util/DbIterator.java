package org.iq80.leveldb.util;

import com.google.common.primitives.Ints;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.MemTable.MemTableIterator;
import org.iq80.leveldb.impl.SeekingIterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public final class DbIterator extends AbstractSeekingIterator<InternalKey, Slice>
{
    private final MemTableIterator memTableIterator;
    private final MemTableIterator immutableMemTableIterator;
    private final List<InternalTableIterator> level0Files;
    private final List<LevelIterator> levels;

    private final PriorityQueue<ComparableIterator> priorityQueue;
    private final Comparator<InternalKey> comparator;

    public DbIterator(MemTableIterator memTableIterator,
            MemTableIterator immutableMemTableIterator,
            List<InternalTableIterator> level0Files,
            List<LevelIterator> levels,
            Comparator<InternalKey> comparator)
    {
        this.memTableIterator = memTableIterator;
        this.immutableMemTableIterator = immutableMemTableIterator;
        this.level0Files = level0Files;
        this.levels = levels;
        this.comparator = comparator;

        this.priorityQueue = new PriorityQueue<ComparableIterator>(3);
        resetPriorityQueue(comparator);
    }

    @Override
    protected void seekToFirstInternal()
    {
        if (memTableIterator != null) {
            memTableIterator.seekToFirst();
        }
        if (immutableMemTableIterator != null) {
            immutableMemTableIterator.seekToFirst();
        }
        for (InternalTableIterator level0File : level0Files) {
            level0File.seekToFirst();
        }
        for (LevelIterator level : levels) {
            level.seekToFirst();
        }
        resetPriorityQueue(comparator);
    }

    @Override
    protected void seekInternal(InternalKey targetKey)
    {
        if (memTableIterator != null) {
            memTableIterator.seek(targetKey);
        }
        if (immutableMemTableIterator != null) {
            immutableMemTableIterator.seek(targetKey);
        }
        for (InternalTableIterator level0File : level0Files) {
            level0File.seek(targetKey);
        }
        for (LevelIterator level : levels) {
            level.seek(targetKey);
        }
        resetPriorityQueue(comparator);
    }

    private void resetPriorityQueue(Comparator<InternalKey> comparator)
    {
        int i = 0;
        if (memTableIterator != null && memTableIterator.hasNext()) {
            priorityQueue.add(new ComparableIterator(memTableIterator, comparator, i++, memTableIterator.next()));
        }
        if (immutableMemTableIterator != null && immutableMemTableIterator.hasNext()) {
            priorityQueue.add(new ComparableIterator(immutableMemTableIterator, comparator, i++, immutableMemTableIterator.next()));
        }
        for (InternalTableIterator level0File : level0Files) {
            if (level0File.hasNext()) {
                priorityQueue.add(new ComparableIterator(level0File, comparator, i++, level0File.next()));
            }
        }
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
        sb.append("DbIterator");
        sb.append("{memTableIterator=").append(memTableIterator);
        sb.append(", immutableMemTableIterator=").append(immutableMemTableIterator);
        sb.append(", level0Files=").append(level0Files);
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
