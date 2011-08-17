package org.iq80.leveldb.table;

import org.iq80.leveldb.util.ChannelBufferComparator;
import org.jboss.netty.buffer.ChannelBuffer;

public class BasicUserComparator extends ChannelBufferComparator implements UserComparator
{
    @Override
    public void findShortestSeparator(
            ChannelBuffer start,
            ChannelBuffer limit)
    {
        // Find length of common prefix
        int sharedBytes = BlockBuilder.calculateSharedBytes(start, limit);

        // Do not shorten if one string is a prefix of the other
        if (sharedBytes < Math.min(start.readableBytes(), limit.readableBytes())) {
            // if we can add one to the last shared byte without overflow and the two keys differ by more than
            // one increment at this location.
            int lastSharedByte = start.getUnsignedByte(sharedBytes);
            if (lastSharedByte < 0xff && lastSharedByte + 1 < limit.getUnsignedByte(sharedBytes)) {
                start.setByte(sharedBytes, lastSharedByte + 1);
                start.writerIndex(sharedBytes + 1);

                assert (compare(start, limit) < 0) : "start must be less than last limit";
            }
        }
    }

    @Override
    public void findShortSuccessor(ChannelBuffer key)
    {
        // Find first character that can be incremented
        for (int i = 0; i < key.readableBytes(); i++) {
            int b = key.getUnsignedByte(i);
            if (b != 0xff) {
                key.setByte(i, b + 1);
                key.writerIndex(i + 1);
            }
        }
        // key is a run of 0xffs.  Leave it alone.
    }
}
