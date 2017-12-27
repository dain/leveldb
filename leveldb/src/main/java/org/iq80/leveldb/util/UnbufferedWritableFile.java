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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * @author Honore Vasconcelos
 */
public class UnbufferedWritableFile implements WritableFile
{
    private final File file;
    private final FileChannel channel;

    private UnbufferedWritableFile(File file, FileChannel channel)
    {
        this.file = file;
        this.channel = channel;
    }

    public static WritableFile open(File file) throws FileNotFoundException
    {
        return new UnbufferedWritableFile(file, new FileOutputStream(file).getChannel());
    }

    @Override
    public void append(Slice data) throws IOException
    {
        channel.write(data.toByteBuffer());
    }

    @Override
    public void force() throws IOException
    {
        channel.force(false);
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
    }

    @Override
    public String toString()
    {
        return "UnbufferedWritableFile{" +
                "file=" + file +
                '}';
    }
}
