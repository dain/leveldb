/*
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

package org.iq80.leveldb.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.google.common.base.Predicate;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Lists;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public final class ReverseIterators
{

    // reimplements several methods and classes from com.google.common.collect.Iterators
    // in addition to further reversing functionality and convenience methods
    // in order to accommodate reverse iteration

    public static <T> ListReverseIterator<T> listReverseIterator(ListIterator<T> listIter)
    {
        return new ListReverseIterator<T>(listIter);
    }

    public static <T> ListReverseIterator<T> listReverseIterator(List<T> list)
    {
        return listReverseIterator(list.listIterator());
    }

    public static <T> ListReverseIterator<T> listReverseIterator(Collection<T> collection)
    {
        return listReverseIterator(Lists.newArrayList(collection));
    }

    public static <T> ReversePeekingIterator<T> reversePeekingIterator(
            ListIterator<? extends T> listIter)
    {
        return reversePeekingIterator(ReverseIterators.listReverseIterator(listIter));
    }

    public static <T> ReversePeekingIterator<T> reversePeekingIterator(
            List<? extends T> list)
    {
        return reversePeekingIterator(ReverseIterators.listReverseIterator(list));
    }

    public static <T> ReversePeekingIterator<T> reversePeekingIterator(
            Collection<? extends T> collection)
    {
        return reversePeekingIterator(ReverseIterators.listReverseIterator(collection));
    }

    public static <T> ReversePeekingIterator<T> reversePeekingIterator(
            ReverseIterator<? extends T> iterator)
    {
        if (iterator instanceof ReversePeekingImpl) {
            @SuppressWarnings("unchecked")
            ReversePeekingImpl<T> rPeeking = (ReversePeekingImpl<T>) iterator;
            return rPeeking;
        }
        return new ReversePeekingImpl<T>(iterator);
    }

    public static <T extends Iterator<?>> Predicate<T> hasNext()
    {
        return new Predicate<T>()
        {
            public boolean apply(T iter)
            {
                return iter.hasNext();
            }
        };
    }

    public static <T extends ReverseIterator<?>> Predicate<T> hasPrev()
    {
        return new Predicate<T>()
        {
            public boolean apply(T iter)
            {
                return iter.hasPrev();
            }
        };
    }

    private static class ListReverseIterator<E>
            implements ReverseIterator<E>
    {
        private final ListIterator<E> iter;

        public ListReverseIterator(ListIterator<E> listIterator)
        {
            this.iter = listIterator;
        }

        @Override
        public boolean hasNext()
        {
            return iter.hasNext();
        }

        @Override
        public E next()
        {
            return iter.next();
        }

        @Override
        public void remove()
        {
            iter.remove();
        }

        @Override
        public E prev()
        {
            return iter.previous();
        }

        @Override
        public boolean hasPrev()
        {
            return iter.hasPrevious();
        }
    }

    private static class ReversePeekingImpl<E>
            implements
            PeekingIterator<E>,
            ReversePeekingIterator<E>
    {
        private final ReverseIterator<? extends E> rIterator;
        private boolean rHasPeeked;
        private E rPeekedElement;
        private boolean hasPeeked;
        private E peekedElement;

        public ReversePeekingImpl(ReverseIterator<? extends E> iterator)
        {
            this.rIterator = checkNotNull(iterator);
        }

        @Override
        public boolean hasNext()
        {
            return hasPeeked || rIterator.hasNext();
        }

        @Override
        public boolean hasPrev()
        {
            return rHasPeeked || rIterator.hasPrev();
        }

        @Override
        public E next()
        {
            hasPeeked = false;
            peekedElement = null;
            E next = rIterator.next();
            rHasPeeked = true;
            rPeekedElement = next;
            return next;
        }

        @Override
        public E prev()
        {
            rHasPeeked = false;
            rPeekedElement = null;
            E prev = rIterator.prev();
            hasPeeked = true;
            peekedElement = prev;
            return prev;
        }

        @Override
        public E peek()
        {
            if (!hasPeeked) {
                peekedElement = rIterator.next();
                rIterator.prev();
                hasPeeked = true;
            }
            return peekedElement;
        }

        @Override
        public E peekPrev()
        {
            if (!rHasPeeked) {
                rPeekedElement = rIterator.prev();
                rIterator.next(); // reset to original position
                rHasPeeked = true;
            }
            return rPeekedElement;
        }

        @Override
        public void remove()
        {
            checkState(!(hasPeeked || rHasPeeked),
                    "Can't remove after peeking at next or previous");
            rIterator.remove();
        }
    }
}
