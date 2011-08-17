package org.iq80.leveldb;

import com.google.common.collect.PeekingIterator;

import java.util.Map.Entry;

public interface SeekingIterator<K,V> extends PeekingIterator<Entry<K, V>>
{
    /**
     * Repositions the iterator so the beginning of this block.
     */
    void seekToFirst();

    /**
     * Repositions the iterator so the key of the next BlockElement returned greater than or equal to the specified targetKey.
     */
    void seek(K targetKey);
}
