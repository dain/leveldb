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

import com.google.common.base.Preconditions;
import org.iq80.leveldb.util.Closeables;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.concurrent.Callable;

/**
 * @author Honore Vasconcelos
 */
public class FileTableDataSource implements TableDataSource
{
    private final String name;
    private final FileChannel fileChannel;
    private final long size;

    public FileTableDataSource(String name, FileChannel fileChannel) throws IOException
    {
        this.name = name;
        this.fileChannel = fileChannel;
        this.size = fileChannel.size();
        Preconditions.checkNotNull(name, "name is null");
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
    public ByteBuffer read(long offset, int length) throws IOException
    {
        ByteBuffer uncompressedBuffer = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(uncompressedBuffer, offset);
        if (uncompressedBuffer.hasRemaining()) {
            throw new IOException("Could not read all the data");
        }
        uncompressedBuffer.clear();
        return uncompressedBuffer;
    }

    @Override
    public Callable<?> closer()
    {
        return new Closer(fileChannel);
    }

    private static class Closer
            implements Callable<Void>
    {
        private final Closeable closeable;

        Closer(Closeable closeable)
        {
            this.closeable = closeable;
        }

        @Override
        public Void call()
        {
            Closeables.closeQuietly(closeable);
            return null;
        }
    }

    @Override
    public String toString()
    {
        return "FileTableDataSource{" +
                "name='" + name + '\'' +
                ", size=" + size +
                '}';
    }
}
