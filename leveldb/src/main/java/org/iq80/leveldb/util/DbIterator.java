package org.iq80.leveldb.util;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.MemTable.MemTableIterator;
import org.iq80.leveldb.impl.SeekingIterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

public final class DbIterator extends AbstractSeekingIterator<InternalKey, Slice> implements InternalIterator
{
    private final MemTableIterator memTableIterator;
    private final MemTableIterator immutableMemTableIterator;
    private final List<InternalTableIterator> level0Files;
    private final List<LevelIterator> levels;

    private final Comparator<InternalKey> comparator;

    private final ComparableIterator[] heap;
    private int heapSize = 0;

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

        this.heap = new ComparableIterator[3 + level0Files.size() + levels.size()];
        resetPriorityQueue();
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
        resetPriorityQueue();
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
        resetPriorityQueue();
    }

    @Override
    protected Entry<InternalKey, Slice> getNextElement()
    {
        if (heapSize == 0) {
            return null;
        }

        ComparableIterator smallest = heap[0];
        Entry<InternalKey, Slice> result = smallest.next();

        // if the smallest iterator has more elements, put it back in the heap,
        // otherwise use the last element in the queue
        ComparableIterator replacementElement;
        if (smallest.hasNext()) {
            replacementElement = smallest;
        }
        else {
            heapSize--;
            replacementElement = heap[heapSize];
            heap[heapSize] = null;
        }

        if (replacementElement != null) {
            heap[0] = replacementElement;
            heapSiftDown(0);
        }

        return result;
    }

    private void resetPriorityQueue()
    {
        int i = 0;
        heapSize = 0;
        if (memTableIterator != null && memTableIterator.hasNext()) {
            heapAdd(new ComparableIterator(memTableIterator, comparator, i++, memTableIterator.next()));
        }
        if (immutableMemTableIterator != null && immutableMemTableIterator.hasNext()) {
            heapAdd(new ComparableIterator(immutableMemTableIterator, comparator, i++, immutableMemTableIterator.next()));
        }
        for (InternalTableIterator level0File : level0Files) {
            if (level0File.hasNext()) {
                heapAdd(new ComparableIterator(level0File, comparator, i++, level0File.next()));
            }
        }
        for (LevelIterator level : levels) {
            if (level.hasNext()) {
                heapAdd(new ComparableIterator(level, comparator, i++, level.next()));
            }
        }
    }

    private boolean heapAdd(ComparableIterator newElement)
    {
        Preconditions.checkNotNull(newElement, "newElement is null");

        heap[heapSize] = newElement;
        heapSiftUp(heapSize++);
        return true;
    }

    private void heapSiftUp(int childIndex)
    {
        ComparableIterator target = heap[childIndex];
        int parentIndex;
        while (childIndex > 0) {
            parentIndex = (childIndex - 1) / 2;
            ComparableIterator parent = heap[parentIndex];
            if (parent.compareTo(target) <= 0) {
                break;
            }
            heap[childIndex] = parent;
            childIndex = parentIndex;
        }
        heap[childIndex] = target;
    }

    private void heapSiftDown(int rootIndex)
    {
        ComparableIterator target = heap[rootIndex];
        int childIndex;
        while ((childIndex = rootIndex * 2 + 1) < heapSize) {
            if (childIndex + 1 < heapSize
                    && heap[childIndex + 1].compareTo(heap[childIndex]) < 0) {
                childIndex++;
            }
            if (target.compareTo(heap[childIndex]) <= 0) {
                break;
            }
            heap[rootIndex] = heap[childIndex];
            rootIndex = childIndex;
        }
        heap[rootIndex] = target;
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
