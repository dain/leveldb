package org.iq80.leveldb.table;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.jboss.netty.buffer.ChannelBuffer;

import java.util.NoSuchElementException;

public class SeekingIterators
{
    public static SeekingIterator concat(SeekingIterator iterator, Function<BlockEntry, SeekingIterator> function)
    {
        Preconditions.checkNotNull(iterator, "inputs is null");

        return new TwoLevelIterator(iterator, function);
    }

    public static SeekingIterator emptyIterator()
    {
        return EMPTY_ITERATOR;
    }

    private static class TwoLevelIterator implements SeekingIterator
    {
        private final SeekingIterator indexIterator;
        private final Function<BlockEntry, SeekingIterator> function;
        private SeekingIterator dataIterator;

        public TwoLevelIterator(SeekingIterator indexIterator, Function<BlockEntry, SeekingIterator> function)
        {
            this.indexIterator = indexIterator;
            this.function = function;
            dataIterator = emptyIterator();
        }

        @Override
        public BlockEntry peek()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return dataIterator.peek();
        }

        @Override
        public void seekToFirst()
        {
            // reset index to before first and clear the data iterator
            indexIterator.seekToFirst();
            dataIterator = emptyIterator();
        }

        @Override
        public void seek(ChannelBuffer targetKey)
        {
            // seek the index to the block containing the key
            indexIterator.seek(targetKey);

            // if indexIterator does not have a next, it mean the key does not exist in this iterator
            if (indexIterator.hasNext()) {
                // load and see the data iterator to the key
                dataIterator = function.apply(indexIterator.next());
                dataIterator.seek(targetKey);
            } else {
                dataIterator = emptyIterator();
            }
        }

        @Override
        public boolean hasNext()
        {
            // http://code.google.com/p/google-collections/issues/detail?id=151
            // current.hasNext() might be relatively expensive, worth minimizing.
            boolean currentHasNext;

            // note: it must be here & not where 'current' is assigned,
            // because otherwise we'll have called inputs.next() before throwing
            // the first NPE, and the next time around we'll call inputs.next()
            // again, incorrectly moving beyond the error.
            while (!(currentHasNext = dataIterator.hasNext()) && indexIterator.hasNext()) {
                dataIterator = function.apply(indexIterator.next());
            }
            return currentHasNext;
        }

        @Override
        public BlockEntry next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return dataIterator.next();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static final SeekingIterator EMPTY_ITERATOR = new SeekingIterator()
    {
        @Override
        public boolean hasNext()
        {
            return false;
        }

        @Override
        public void seekToFirst()
        {
        }

        @Override
        public void seek(ChannelBuffer targetKey)
        {
        }

        @Override
        public BlockEntry peek()
        {
            throw new NoSuchElementException();
        }

        @Override
        public BlockEntry next()
        {
            throw new NoSuchElementException();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    };

}
