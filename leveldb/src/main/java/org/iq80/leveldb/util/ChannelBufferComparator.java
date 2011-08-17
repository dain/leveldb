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

import org.jboss.netty.buffer.ChannelBuffer;

import java.util.Comparator;

public class ChannelBufferComparator implements Comparator<ChannelBuffer>
{
    public static final ChannelBufferComparator CHANNEL_BUFFER_COMPARATOR = new ChannelBufferComparator();

    @Override
    public int compare(ChannelBuffer bufferA, ChannelBuffer bufferB)
    {
        if (bufferA.isDirect() || bufferB.isDirect()) {
            int aReadableBytes = bufferA.readableBytes();
            int bReadableBytes = bufferB.readableBytes();
            int minSize = Math.min(aReadableBytes, bReadableBytes);

            int aBase = bufferA.readerIndex();
            int bBase = bufferB.readerIndex();

            for (int i = 0; i < minSize; i++) {
                int v1 = bufferA.getUnsignedByte(aBase + i);
                int v2 = bufferB.getUnsignedByte(bBase + i);

                if (v1 != v2) {
                    return v1 - v2;
                }
            }
            return aReadableBytes - bReadableBytes;
        }
        else {
            byte[] aArray = bufferA.array();
            byte[] bArray = bufferB.array();

            int aBase = bufferA.arrayOffset() + bufferA.readerIndex();
            int bBase = bufferB.arrayOffset() + bufferB.readerIndex();

            int aReadableBytes = bufferA.readableBytes();
            int bReadableBytes = bufferB.readableBytes();
            int minSize = Math.min(aReadableBytes, bReadableBytes);

            for (int i = 0; i < minSize; i++) {
                int v1 = aArray[aBase++] & 0xFF;
                int v2 = bArray[bBase++] & 0xFF;

                if (v1 != v2) {
                    return v1 - v2;
                }
            }
            return aReadableBytes - bReadableBytes;
        }
    }
}
