package org.iq80.leveldb.table;

public interface SeekingIterable extends Iterable<BlockEntry>
{
    @Override
    SeekingIterator iterator();
}
