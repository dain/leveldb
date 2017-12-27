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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Memory mapped file implementation of {@link WritableFile}.
 */
public class MMWritableFile implements WritableFile
{
    private final File file;
    private final int pageSize;
    private MappedByteBuffer mappedByteBuffer;
    private int fileOffset;

    private MMWritableFile(File file, int pageSize, MappedByteBuffer map)
    {
        this.file = file;
        this.pageSize = pageSize;
        this.fileOffset = 0;
        this.mappedByteBuffer = map;
    }

    public static WritableFile open(File file, int pageSize) throws IOException
    {
        return new MMWritableFile(file, pageSize, Files.map(file, FileChannel.MapMode.READ_WRITE, pageSize));
    }

    @Override
    public void append(Slice data) throws IOException
    {
        ensureCapacity(data.length());
        data.getBytes(0, mappedByteBuffer);
    }

    private void destroyMappedByteBuffer()
    {
        if (mappedByteBuffer != null) {
            fileOffset += mappedByteBuffer.position();
            unmap();
        }
        mappedByteBuffer = null;
    }

    private void ensureCapacity(int bytes)
            throws IOException
    {
        if (mappedByteBuffer == null) {
            mappedByteBuffer = openNewMap(fileOffset, Math.max(bytes, pageSize));
        }
        if (mappedByteBuffer.remaining() < bytes) {
            // remap
            fileOffset += mappedByteBuffer.position();
            unmap();
            int sizeToGrow = Math.max(bytes, pageSize);
            mappedByteBuffer = openNewMap(fileOffset, sizeToGrow);
        }
    }

    private MappedByteBuffer openNewMap(int fileOffset, int sizeToGrow) throws IOException
    {
        FileChannel cha = null;
        try {
            cha = openChannel();
            return cha.map(FileChannel.MapMode.READ_WRITE, fileOffset, sizeToGrow);
        }
        finally {
            Closeables.closeQuietly(cha);
        }
    }

    private FileChannel openChannel() throws FileNotFoundException
    {
        return new java.io.RandomAccessFile(file, "rw").getChannel();
    }

    private void unmap()
    {
        ByteBufferSupport.unmap(mappedByteBuffer);
    }

    @Override
    public void force() throws IOException
    {

    }

    @Override
    public void close() throws IOException
    {
        destroyMappedByteBuffer();
        FileChannel cha = null;
        try {
            cha = openChannel();
            cha.truncate(fileOffset);
        }
        finally {
            Closeables.closeQuietly(cha);
        }
    }

    @Override
    public String toString()
    {
        return "MMWritableFile{" +
                "file=" + file +
                '}';
    }
}
