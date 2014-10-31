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

import com.google.common.base.Throwables;
import sun.nio.ch.FileChannelImpl;

import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;

public final class ByteBufferSupport
{
    private static final Method unmap;

    static {
        Method x;
        try {
            x = FileChannelImpl.class.getDeclaredMethod("unmap", MappedByteBuffer.class);
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
        x.setAccessible(true);
        unmap = x;
    }

    private ByteBufferSupport()
    {
    }

    public static void unmap(MappedByteBuffer buffer)
    {
        try {
            unmap.invoke(null, buffer);
        }
        catch (Exception ignored) {
            throw Throwables.propagate(ignored);
        }
    }
}
