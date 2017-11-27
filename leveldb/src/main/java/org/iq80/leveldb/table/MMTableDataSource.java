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

package org.iq80.leveldb.table;

import org.iq80.leveldb.util.ByteBufferSupport;
import org.iq80.leveldb.util.Closeables;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Callable;

/**
 * Memory mapped filed table.
 *
 * @author Honore Vasconcelos
 */
public class MMTableDataSource implements TableDataSource
{
    private final String name;
    private final FileChannel fileChannel;
    private final long size;
    private final MappedByteBuffer data;

    public MMTableDataSource(String name, FileChannel fileChannel) throws IOException
    {
        this.name = name;
        this.fileChannel = fileChannel;
        this.size = fileChannel.size();
        this.data = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size);
    }

    @Override
    public String name()
    {
        return this.name;
    }

    @Override
    public long size()
    {
        return size;
    }

    @Override
    public ByteBuffer read(long offset, int length)
    {
        int newPosition = (int) (data.position() + offset);
        return (ByteBuffer) data.duplicate().order(ByteOrder.LITTLE_ENDIAN).clear().limit(newPosition + length).position(newPosition);
    }

    @Override
    public Callable<?> closer()
    {
        return new Closer(fileChannel, data);
    }

    private static class Closer
            implements Callable<Void>
    {
        // private final String name;
        private final Closeable closeable;
        private final MappedByteBuffer data;

        Closer(Closeable closeable, MappedByteBuffer data)
        {
            this.closeable = closeable;
            this.data = data;
        }

        public Void call()
        {
            ByteBufferSupport.unmap(data);
            Closeables.closeQuietly(closeable);
            return null;
        }
    }

    @Override
    public String toString()
    {
        return "MMTableDataSource{" +
                "name='" + name + '\'' +
                ", size=" + size +
                '}';
    }
}
