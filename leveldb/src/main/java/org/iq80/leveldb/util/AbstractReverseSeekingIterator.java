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

package org.iq80.leveldb.util;

import org.iq80.leveldb.impl.ReverseSeekingIterator;

import java.util.Map.Entry;
import java.util.NoSuchElementException;

public abstract class AbstractReverseSeekingIterator<K, V>
        implements ReverseSeekingIterator<K, V>
{
    private Entry<K, V> rPeekedElement;
    private Entry<K, V> peekedElement;

    @Override
    public final void seekToFirst()
    {
        rPeekedElement = peekedElement = null;
        seekToFirstInternal();
    }

    @Override
    public void seekToLast()
    {
        rPeekedElement = peekedElement = null;
        seekToLastInternal();
    }

    @Override
    public final void seek(K targetKey)
    {
        rPeekedElement = peekedElement = null;
        seekInternal(targetKey);
    }

    @Override
    public final void seekToEnd()
    {
        rPeekedElement = peekedElement = null;
        seekToEndInternal();
    }

    @Override
    public final boolean hasNext()
    {
        return peekedElement != null || hasNextInternal();
    }

    @Override
    public final boolean hasPrev()
    {
        return rPeekedElement != null || hasPrevInternal();
    }

    @Override
    public final Entry<K, V> next()
    {
        peekedElement = null;
        Entry<K, V> next = getNextElement();
        if (next == null) {
            throw new NoSuchElementException();
        }
        rPeekedElement = next;
        return next;
    }

    @Override
    public final Entry<K, V> prev()
    {
        rPeekedElement = null;
        Entry<K, V> prev = getPrevElement();
        if (prev == null) {
            throw new NoSuchElementException();
        }
        peekedElement = prev;
        return prev;
    }

    @Override
    public final Entry<K, V> peek()
    {
        if (peekedElement == null) {
            peekedElement = peekInternal();
            if (peekedElement == null) {
                throw new NoSuchElementException();
            }
        }
        return peekedElement;
    }

    @Override
    public final Entry<K, V> peekPrev()
    {
        if (rPeekedElement == null) {
            rPeekedElement = peekPrevInternal();
            if (rPeekedElement == null) {
                throw new NoSuchElementException();
            }
        }
        return rPeekedElement;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    // non-abstract; in case the iterator implementation provides
    // a more efficient means of peeking, it can override this method
    protected Entry<K, V> peekInternal()
    {
        Entry<K, V> ret = getNextElement();
        if (ret == null) {
            throw new NoSuchElementException();
        }
        getPrevElement();
        return ret;
    }

    protected Entry<K, V> peekPrevInternal()
    {
        Entry<K, V> ret = getPrevElement();
        if (ret == null) {
            throw new NoSuchElementException();
        }
        getNextElement();
        return ret;
    }

    protected abstract void seekToLastInternal();

    protected abstract void seekToFirstInternal();

    protected abstract void seekToEndInternal();

    protected abstract void seekInternal(K targetKey);

    protected abstract boolean hasNextInternal();

    protected abstract boolean hasPrevInternal();

    protected abstract Entry<K, V> getNextElement();

    protected abstract Entry<K, V> getPrevElement();
}
