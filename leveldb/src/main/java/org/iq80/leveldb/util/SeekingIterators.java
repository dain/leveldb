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
import org.iq80.leveldb.impl.SeekingIterator;

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
