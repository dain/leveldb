package org.iq80.leveldb.impl;

import com.google.common.collect.ComparisonChain;
import org.iq80.leveldb.table.UserComparator;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

public class InternalKeyComparator implements Comparator<InternalKey>
{
    private final UserComparator userComparator;

    public InternalKeyComparator(UserComparator userComparator)
    {
        this.userComparator = userComparator;
    }

    public UserComparator getUserComparator()
    {
        return userComparator;
    }

    @Override
    public int compare(InternalKey left, InternalKey right)
    {
        return ComparisonChain.start()
                .compare(left.getUserKey(), right.getUserKey(), userComparator)
                .compare(right.getSequenceNumber(), left.getSequenceNumber()) // reverse sorted version numbers
                .result();
    }

    /**
     * Returns {@code true} if each element in {@code iterable} after the first is
     * greater than or equal to the element that preceded it, according to this
     * ordering. Note that this is always true when the iterable has fewer than
     * two elements.
     */
    public boolean isOrdered(InternalKey... keys)
    {
        return isOrdered(Arrays.asList(keys));
    }

    /**
     * Returns {@code true} if each element in {@code iterable} after the first is
     * greater than or equal to the element that preceded it, according to this
     * ordering. Note that this is always true when the iterable has fewer than
     * two elements.
     */
    public boolean isOrdered(Iterable<InternalKey> keys)
    {
        Iterator<InternalKey> iterator = keys.iterator();
        if (!iterator.hasNext()) {
            return true;
        }

        InternalKey previous = iterator.next();
        while (iterator.hasNext()) {
            InternalKey next = iterator.next();
            if (compare(previous, next) > 0) {
                return false;
            }
            previous = next;
        }
        return true;
    }
}
