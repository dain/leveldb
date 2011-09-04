package org.iq80.leveldb.table;

import org.iq80.leveldb.util.Slice;

import java.util.Comparator;

// todo this interface needs more thought
public interface UserComparator extends Comparator<Slice>
{
    String name();

    Slice findShortestSeparator(Slice start, Slice limit);

    Slice findShortSuccessor(Slice key);
}
