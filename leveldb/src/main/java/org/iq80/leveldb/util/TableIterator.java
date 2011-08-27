package org.iq80.leveldb.util;

import org.iq80.leveldb.table.Block;
import org.iq80.leveldb.table.BlockIterator;
import org.iq80.leveldb.table.Table;

import java.util.Map.Entry;

public final class TableIterator extends AbstractSeekingIterator<Slice, Slice>
{
    private final Table table;
    private final BlockIterator blockIterator;
    private BlockIterator current;

    public TableIterator(Table table, BlockIterator blockIterator)
    {
        this.table = table;
        this.blockIterator = blockIterator;
        current = null;
    }

    @Override
    protected void seekToFirstInternal()
    {
        // reset index to before first and clear the data iterator
        blockIterator.seekToFirst();
        current = null;
    }

    @Override
    protected void seekInternal(Slice targetKey)
    {
        // seek the index to the block containing the key
        blockIterator.seek(targetKey);

        // if indexIterator does not have a next, it mean the key does not exist in this iterator
        if (blockIterator.hasNext()) {
            // seek the current iterator to the key
            current = getNextBlock();
            current.seek(targetKey);
        }
        else {
            current = null;
        }
    }

    @Override
    protected Entry<Slice, Slice> getNextElement()
    {
        // note: it must be here & not where 'current' is assigned,
        // because otherwise we'll have called inputs.next() before throwing
        // the first NPE, and the next time around we'll call inputs.next()
        // again, incorrectly moving beyond the error.
        boolean currentHasNext = false;
        while (true) {
            if (current != null) {
                currentHasNext = current.hasNext();
            }
            if (!(currentHasNext)) {
                if (blockIterator.hasNext()) {
                    current = getNextBlock();
                }
                else {
                    break;
                }
            }
            else {
                break;
            }
        }
        if (currentHasNext) {
            return current.next();
        }
        else {
            // set current to empty iterator to avoid extra calls to user iterators
            current = null;
            return null;
        }
    }

    private BlockIterator getNextBlock()
    {
        Slice blockHandle = blockIterator.next().getValue();
        Block dataBlock = table.openBlock(blockHandle);
        return dataBlock.iterator();
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("ConcatenatingIterator");
        sb.append("{blockIterator=").append(blockIterator);
        sb.append(", current=").append(current);
        sb.append('}');
        return sb.toString();
    }
}
