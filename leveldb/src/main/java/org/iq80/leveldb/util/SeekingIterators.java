/**
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.leveldb.util;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import org.iq80.leveldb.SeekingIterator;

import java.util.Comparator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

public class SeekingIterators
{
    /**
     * Combines multiple iterators into a single iterator by concatenating the {@code inputs}
     * iterators one after the other. The input iterators are not polled until necessary.
     * {@code NullPointerException} if any of the input iterators are null.
     */
    public static <K, V> SeekingIterator<K, V> concat(SeekingIterator<K, ? extends SeekingIterator<K, ? extends V>> inputs)
    {
        return new ConcatenatingIterator<K, V>(inputs);
    }

    private static final class ConcatenatingIterator<K, V> implements SeekingIterator<K, V>
    {
        private final SeekingIterator<K, ? extends SeekingIterator<K, ? extends V>> inputs;
        private SeekingIterator<K, ? extends V> current;

        public ConcatenatingIterator(SeekingIterator<K, ? extends SeekingIterator<K, ? extends V>> inputs)
        {
            this.inputs = inputs;
            current = emptyIterator();
        }

        @Override
        public Entry<K, V> peek()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            // this woks because Entry is unmodifiable
            return (Entry<K, V>) current.peek();
        }

        @Override
        public void seekToFirst()
        {
            // reset index to before first and clear the data iterator
            inputs.seekToFirst();
            current = emptyIterator();
        }

        @Override
        public void seek(K targetKey)
        {
            // seek the index to the block containing the key
            inputs.seek(targetKey);

            // if indexIterator does not have a next, it mean the key does not exist in this iterator
            if (inputs.hasNext()) {
                // load and see the data iterator to the key
                current = inputs.next().getValue();
                current.seek(targetKey);
            }
            else {
                current = emptyIterator();
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
            while (!(currentHasNext = current.hasNext()) && inputs.hasNext()) {
                current = inputs.next().getValue();
            }
            return currentHasNext;
        }

        @Override
        public Entry<K, V> next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            // this woks because Entry is unmodifiable
            return (Entry<K, V>) current.next();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
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

    /**
     * Combines multiple iterators into a single iterator by merging the values from the {@code inputs}
     * iterators in natural sorted order.  The supplied inputs are assumed to be natural sorted.
     * {@code NullPointerException} if any of the input iterators are null.
     */
    public static <K extends Comparable<K>, V> SeekingIterator<K, V> merge(Iterable<? extends SeekingIterator<K, ? extends V>> inputs)
    {
        return new MergingIterator<K, V>(inputs, Ordering.<K>natural());
    }

    /**
     * Combines multiple iterators into a single iterator by merging the values from the {@code inputs}
     * iterators in sorted order as specified by the comparator.  The supplied inputs are assumed to be
     * sorted by the specified comparator.
     * {@code NullPointerException} if any of the input iterators are null.
     */
    public static <K, V> SeekingIterator<K, V> merge(Iterable<? extends SeekingIterator<K, ? extends V>> inputs, Comparator<K> comparator)
    {
        int size = Iterables.size(inputs);
        if (size == 0) {
            return emptyIterator();
        } else if (size == 1) {
            return (SeekingIterator<K, V>) Iterables.getOnlyElement(inputs);
        } else {
            return new MergingIterator<K, V>(inputs, comparator);
        }
    }

    private static final class MergingIterator<K, V> implements SeekingIterator<K, V>
    {
        private final Iterable<? extends SeekingIterator<K, ? extends V>> inputs;
        private final Comparator<K> comparator;
        private SeekingIterator<K, ? extends V> current;

        public MergingIterator(Iterable<? extends SeekingIterator<K, ? extends V>> inputs, Comparator<K> comparator)
        {
            this.inputs = inputs;
            this.comparator = comparator;
            findSmallestChild();
        }

        @Override
        public Entry<K, V> peek()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return (Entry<K, V>) current.peek();
        }

        @Override
        public void seekToFirst()
        {
            for (SeekingIterator<K, ? extends V> input : inputs) {
                input.seekToFirst();
            }
            findSmallestChild();
        }

        @Override
        public void seek(K targetKey)
        {
            for (SeekingIterator<K, ? extends V> input : inputs) {
                input.seek(targetKey);
            }
            findSmallestChild();
        }

        @Override
        public boolean hasNext()
        {
            return current != null;
        }

        @Override
        public Entry<K, V> next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Entry<K, V> result = (Entry<K, V>) current.next();
            findSmallestChild();
            return result;
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        /**
         * Assigns current to the iterator that will return the smallest value for next, or null
         * if all of the iterators are exhausted.
         */
        private void findSmallestChild()
        {
            K smallestKey = null;
            SeekingIterator<K, ? extends V> smallest = null;
            for (SeekingIterator<K, ? extends V> input : inputs) {
                if (input.hasNext()) {
                    K nextKey = input.peek().getKey();
                    if (smallestKey == null) {
                        smallestKey = nextKey;
                        smallest = input;
                    }
                    else if (comparator.compare(nextKey, smallestKey) < 0) {
                        smallestKey = nextKey;
                        smallest = input;
                    }
                }
            }
            current = smallest;
        }

        @Override
        public String toString()
        {
            final StringBuilder sb = new StringBuilder();
            sb.append("MergingIterator");
            sb.append("{inputs=").append(Iterables.toString(inputs));
            sb.append(", comparator=").append(comparator);
            sb.append(", current=").append(current);
            sb.append('}');
            return sb.toString();
        }
    }

    /**
     * Returns an iterator that applies {@code function} to each element of {@code
     * fromIterator}.
     */
    public static <K, V1, V2> SeekingIterator<K, V2> transformValues(
            SeekingIterator<K, V1> fromIterator,
            final Function<V1, V2> function)
    {
        Preconditions.checkNotNull(fromIterator, "fromIterator is null");
        Preconditions.checkNotNull(function, "function is null");


        Function<Entry<K, V1>, Entry<K, V2>> entryEntryFunction = new Function<Entry<K, V1>, Entry<K, V2>>()
        {
            @Override
            public Entry<K, V2> apply(Entry<K, V1> input)
            {
                K key = input.getKey();
                V2 value = function.apply(input.getValue());
                return Maps.immutableEntry(key, value);
            }
        };
        return new TransformingSeekingIterator<K, V1, K, V2>(fromIterator, entryEntryFunction, Functions.<K>identity());
    }

    
    public static <K1, K2, V> SeekingIterator<K2, V> transformKeys(
            SeekingIterator<K1, V> fromIterator,
            final Function<K1, K2> keyFunction,
            Function<K2, K1> reverseKeyFunction)
    {
        Preconditions.checkNotNull(fromIterator, "fromIterator is null");
        Preconditions.checkNotNull(keyFunction, "keyFunction is null");
        Preconditions.checkNotNull(reverseKeyFunction, "reverseKeyFunction is null");

        Function<Entry<K1, V>, Entry<K2, V>> entryEntryFunction = new Function<Entry<K1, V>, Entry<K2, V>>()
        {
            @Override
            public Entry<K2, V> apply(Entry<K1, V> input)
            {
                K2 key = keyFunction.apply(input.getKey());
                V value = input.getValue();
                return Maps.immutableEntry(key, value);
            }
        };
        return new TransformingSeekingIterator<K1, V, K2, V>(fromIterator, entryEntryFunction, reverseKeyFunction);
    }

    
    /**
     * Returns an iterator that applies {@code function} to each element of {@code
     * fromIterator}.
     */
    public static <K1, V1, K2, V2> SeekingIterator<K2, V2> transformEntries(
            SeekingIterator<K1, V1> fromIterator,
            Function<Entry<K1, V1>, Entry<K2, V2>> entryFunction,
            Function<? super K2, ? extends K1> reverseKeyFunction)
    {
        Preconditions.checkNotNull(fromIterator, "fromIterator is null");
        Preconditions.checkNotNull(entryFunction, "entryFunction is null");
        Preconditions.checkNotNull(reverseKeyFunction, "reverseKeyFunction is null");

        return new TransformingSeekingIterator<K1, V1, K2, V2>(fromIterator, entryFunction, reverseKeyFunction);
    }

    // split into a key and separate value transformer since this is pretty unreadable
    private static class TransformingSeekingIterator<K1, V1, K2, V2> implements SeekingIterator<K2, V2>
    {
        private final SeekingIterator<K1, V1> fromIterator;
        private final Function<Entry<K1, V1>, Entry<K2, V2>> entryFunction;
        private final Function<? super K2, ? extends K1> reverseKeyFunction;

        public TransformingSeekingIterator(SeekingIterator<K1, V1> fromIterator,
                Function<Entry<K1, V1>, Entry<K2, V2>> entryFunction,
                Function<? super K2, ? extends K1> reverseKeyFunction)
        {
            this.fromIterator = fromIterator;
            this.entryFunction = entryFunction;
            this.reverseKeyFunction = reverseKeyFunction;
        }

        @Override
        public void seekToFirst()
        {
            fromIterator.seekToFirst();
        }

        @Override
        public void seek(K2 targetKey)
        {
            fromIterator.seek(reverseKeyFunction.apply(targetKey));
        }

        @Override
        public boolean hasNext()
        {
            return fromIterator.hasNext();
        }

        @Override
        public Entry<K2, V2> peek()
        {
            Entry<K1, V1> from = fromIterator.peek();
            return entryFunction.apply(from);
        }

        @Override
        public Entry<K2, V2> next()
        {
            Entry<K1, V1> from = fromIterator.next();
            return entryFunction.apply(from);
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            final StringBuilder sb = new StringBuilder();
            sb.append("TransformingSeekingIterator");
            sb.append("{fromIterator=").append(fromIterator);
            sb.append(", entryFunction=").append(entryFunction);
            sb.append(", reverseKeyFunction=").append(reverseKeyFunction);
            sb.append('}');
            return sb.toString();
        }
    }

    public static <K, V> SeekingIterator<K, V> emptyIterator()
    {
        return (SeekingIterator<K, V>) EMPTY_ITERATOR;
    }

    private static final EmptySeekingIterator EMPTY_ITERATOR = new EmptySeekingIterator();

    private static final class EmptySeekingIterator implements SeekingIterator<Object, Object>
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
        public void seek(Object targetKey)
        {
        }

        @Override
        public Entry<Object, Object> peek()
        {
            throw new NoSuchElementException();
        }

        @Override
        public Entry<Object, Object> next()
        {
            throw new NoSuchElementException();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

}
