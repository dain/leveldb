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
package org.iq80.leveldb.table;

public class Options
{
    private boolean createIfMissing = true;
    private boolean errorIfExists;
    private int writeBufferSize = 4 << 20;

    private int maxOpenFiles = 1000;

    private int blockRestartInterval = 1;
    private int blockSize = 4 * 1024;
    private CompressionType compressionType = CompressionType.SNAPPY;
    private boolean verifyChecksums = true;

    public boolean isCreateIfMissing()
    {
        return createIfMissing;
    }

    public Options setCreateIfMissing(boolean createIfMissing)
    {
        this.createIfMissing = createIfMissing;
        return this;
    }

    public boolean isErrorIfExists()
    {
        return errorIfExists;
    }

    public Options setErrorIfExists(boolean errorIfExists)
    {
        this.errorIfExists = errorIfExists;
        return this;
    }

    public int getWriteBufferSize()
    {
        return writeBufferSize;
    }

    public Options setWriteBufferSize(int writeBufferSize)
    {
        this.writeBufferSize = writeBufferSize;
        return this;
    }

    public int getMaxOpenFiles()
    {
        return maxOpenFiles;
    }

    public Options setMaxOpenFiles(int maxOpenFiles)
    {
        this.maxOpenFiles = maxOpenFiles;
        return this;
    }

    public int getBlockRestartInterval()
    {
        return blockRestartInterval;
    }

    public Options setBlockRestartInterval(int blockRestartInterval)
    {
        this.blockRestartInterval = blockRestartInterval;
        return this;
    }

    public int getBlockSize()
    {
        return blockSize;
    }

    public Options setBlockSize(int blockSize)
    {
        this.blockSize = blockSize;
        return this;
    }

    public CompressionType getCompressionType()
    {
        return compressionType;
    }

    public Options setCompressionType(CompressionType compressionType)
    {
        this.compressionType = compressionType;
        return this;
    }

    public boolean isVerifyChecksums()
    {
        return verifyChecksums;
    }

    public Options setVerifyChecksums(boolean verifyChecksums)
    {
        this.verifyChecksums = verifyChecksums;
        return this;
    }
}
