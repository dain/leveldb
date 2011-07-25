package org.iq80.leveldb.table;

import com.google.common.collect.PeekingIterator;
import org.jboss.netty.buffer.ChannelBuffer;

public interface SeekingIterator extends PeekingIterator<BlockEntry>
{
    /**
     * Repositions the iterator so the beginning of this block.
     */
    void seekToFirst();

    /**
     * Repositions the iterator so the key of the next BlockElement returned greater than or equal to the specified targetKey.
     */
    void seek(ChannelBuffer targetKey);
}
