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

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/**
 * @author Honore Vasconcelos
 */
public class UnbufferedRandomInputFile implements RandomInputFile
{
    private final String file;
    private final FileChannel fileChannel;
    private final long size;

    private UnbufferedRandomInputFile(String file, FileChannel fileChannel, long size)
    {
        this.file = file;
        this.fileChannel = fileChannel;
        this.size = size;
    }

    public static RandomInputFile open(File file) throws IOException
    {
        Preconditions.checkNotNull(file, "file is null");
        FileChannel channel = new FileInputStream(file).getChannel();
        return new UnbufferedRandomInputFile(file.getAbsolutePath(), channel, channel.size());
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
    public void close() throws IOException
    {
        fileChannel.close();
    }

    @Override
    public String toString()
    {
        return "FileTableDataSource{" +
                "file='" + file + '\'' +
                ", size=" + size +
                '}';
    }
}
