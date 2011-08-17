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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

public final class Buffers
{
    /**
     * A buffer whose capacity is {@code 0}.
     */
    public static final ChannelBuffer EMPTY_BUFFER = org.jboss.netty.buffer.ChannelBuffers.EMPTY_BUFFER;

    private Buffers()
    {
    }

    public static ChannelBuffer readLengthPrefixedBytes(ChannelBuffer buffer)
    {
        int length = VariableLengthQuantity.unpackInt(buffer);
        return buffer.readBytes(length);
    }

    public static void writeLengthPrefixedBytes(ChannelBuffer buffer, ChannelBuffer value)
    {
        VariableLengthQuantity.packInt(value.readableBytes(), buffer);
        buffer.writeBytes(value);
    }



    public static ByteBuffer allocateByteBuffer(int capacity)
    {
        return ByteBuffer.allocate(capacity).order(LITTLE_ENDIAN);
    }

    public static ByteBuffer byteBufferWrap(byte[] bytes)
    {
        return ByteBuffer.wrap(bytes).order(LITTLE_ENDIAN);
    }

    public static ChannelBuffer buffer(int capacity)
    {
        return org.jboss.netty.buffer.ChannelBuffers.buffer(LITTLE_ENDIAN, capacity);
    }

    public static ChannelBuffer directBuffer(int capacity)
    {
        return org.jboss.netty.buffer.ChannelBuffers.directBuffer(LITTLE_ENDIAN, capacity);
    }

    public static ChannelBuffer dynamicBuffer()
    {
        return org.jboss.netty.buffer.ChannelBuffers.dynamicBuffer(LITTLE_ENDIAN, 256);
    }

    public static ChannelBuffer dynamicBuffer(int estimatedLength)
    {
        return org.jboss.netty.buffer.ChannelBuffers.dynamicBuffer(LITTLE_ENDIAN, estimatedLength);
    }

    public static ChannelBuffer wrappedBuffer(byte[] array)
    {
        return org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer(LITTLE_ENDIAN, array);
    }

    public static ChannelBuffer wrappedBuffer(ByteBuffer buffer)
    {
        return org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer(buffer);
    }

    public static ChannelBuffer wrappedBuffer(ChannelBuffer... buffers)
    {
        return org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer(buffers);
    }

    public static ChannelBuffer copiedBuffer(ChannelBuffer buffer)
    {
        return org.jboss.netty.buffer.ChannelBuffers.copiedBuffer(buffer);
    }

    public static ChannelBuffer copiedBuffer(CharSequence string, Charset charset)
    {
        return org.jboss.netty.buffer.ChannelBuffers.copiedBuffer(LITTLE_ENDIAN, string, charset);
    }

    public static ChannelBuffer unmodifiableBuffer(ChannelBuffer buffer)
    {
        return org.jboss.netty.buffer.ChannelBuffers.unmodifiableBuffer(buffer);
    }
}
