package org.iq80.leveldb.util;

import org.jboss.netty.buffer.ChannelBuffer;

import java.util.Comparator;

import static org.jboss.netty.buffer.ChannelBuffers.swapLong;

public class ChannelBufferComparator implements Comparator<ChannelBuffer>
{
    public static final ChannelBufferComparator CHANNEL_BUFFER_COMPARATOR = new ChannelBufferComparator();

    @Override
    public int compare(ChannelBuffer bufferA, ChannelBuffer bufferB) {
        final int aLen = bufferA.readableBytes();
        final int bLen = bufferB.readableBytes();
        final int minLength = Math.min(aLen, bLen);
        final int longCount = minLength >>> 3;
        final int byteCount = minLength & 7;

        int aIndex = bufferA.readerIndex();
        int bIndex = bufferB.readerIndex();

        if (bufferA.order() == bufferB.order()) {
            for (int i = longCount; i > 0; i --) {
                long va = bufferA.getLong(aIndex);
                long vb = bufferB.getLong(bIndex);
                if (va != vb) {

                    // if upper match, mask upper and compare
                    // else shift upper to lower
                    if (va >>> 32 == vb >>> 32) {
                        va = va & 0xFFFFFFFFL;
                        vb = vb & 0xFFFFFFFFL;
                    } else {
                        va = va >>> 32;
                        vb = vb >>> 32;
                    }

                    if (va > vb) {
                        return 1;
                    } else if (va < vb) {
                        return -1;
                    }
                }
                aIndex += 8;
                bIndex += 8;
            }
        } else {
            for (int i = longCount; i > 0; i --) {
                long va = bufferA.getLong(aIndex);
                long vb = swapLong(bufferB.getLong(bIndex));
                if (va != vb) {

                    // if upper match, mask upper and compare
                    // else shift upper to lower
                    if (va >>> 32 == vb >>> 32) {
                        va = va & 0xFFFFFFFFL;
                        vb = vb & 0xFFFFFFFFL;
                    } else {
                        va = va >>> 32;
                        vb = vb >>> 32;
                    }

                    if (va > vb) {
                        return 1;
                    } else if (va < vb) {
                        return -1;
                    }
                }
                aIndex += 8;
                bIndex += 8;
            }
        }

        for (int i = byteCount; i > 0; i --) {
            int va = bufferA.getUnsignedByte(aIndex);
            int vb = bufferB.getUnsignedByte(bIndex);
            if (va > vb) {
                return 1;
            } else if (va < vb) {
                return -1;
            }
            aIndex ++;
            bIndex ++;
        }

        return aLen - bLen;
    }
}
