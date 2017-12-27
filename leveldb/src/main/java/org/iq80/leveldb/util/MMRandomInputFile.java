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

import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;

import static java.util.Objects.requireNonNull;

/**
 * Memory mapped filed table.
 *
 * @author Honore Vasconcelos
 */
public class MMRandomInputFile implements RandomInputFile
{
    private final String file;
    private final long size;
    private final MappedByteBuffer data;

    private MMRandomInputFile(String file, MappedByteBuffer data, long size)
    {
        this.file = file;
        this.size = size;
        this.data = data;
    }

    /**
     * Open file using memory mapped file access.
     * @param file file to open
     * @return readable file
     * @throws IOException If some other I/O error occurs
     */
    public static RandomInputFile open(File file) throws IOException
    {
        requireNonNull(file, "file is null");
        MappedByteBuffer map = Files.map(file);

        return new MMRandomInputFile(file.getAbsolutePath(), map, map.capacity());
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
    public void close() throws IOException
    {
        try {
            ByteBufferSupport.unmap(data);
        }
        catch (Exception e) {
            throw new IOException("Unable to unmap file", e);
        }
    }

    @Override
    public String toString()
    {
        return "MMTableDataSource{" +
                "file='" + file + '\'' +
                ", size=" + size +
                '}';
    }
}
