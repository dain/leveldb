package org.iq80.leveldb.table;

import org.jboss.netty.buffer.ChannelBuffer;

import java.util.Comparator;

// todo this interface needs more thought
public interface UserComparator extends Comparator<ChannelBuffer>
{
    void findShortestSeparator(ChannelBuffer start, ChannelBuffer limit);

    void findShortSuccessor(ChannelBuffer key);
}
