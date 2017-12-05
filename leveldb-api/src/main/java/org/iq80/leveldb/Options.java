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
package org.iq80.leveldb;

public class Options
{
    private boolean createIfMissing = true;
    private boolean errorIfExists;
    private int writeBufferSize = 4 << 20;

    private int maxOpenFiles = 1000;

    private int blockRestartInterval = 16;
    private int blockSize = 4 * 1024;
    private CompressionType compressionType = CompressionType.SNAPPY;
    private boolean verifyChecksums = true;
    private boolean paranoidChecks;
    private DBComparator comparator;
    private Logger logger;
    private long cacheSize;
    private boolean allowMmapReads = true;
    private boolean allowMmapWrites = true;
    private XFilterPolicy filterPolicy;

    public static Options fromOptions(Options options)
    {
        final Options options1 = new Options();
        options1.createIfMissing = options.createIfMissing;
        options1.errorIfExists = options.errorIfExists;
        options1.writeBufferSize = options.writeBufferSize;
        options1.maxOpenFiles = options.maxOpenFiles;
        options1.blockRestartInterval = options.blockRestartInterval;
        options1.blockSize = options.blockSize;
        options1.compressionType = options.compressionType;
        options1.verifyChecksums = options.verifyChecksums;
        options1.paranoidChecks = options.paranoidChecks;
        options1.comparator = options.comparator;
        options1.logger = options.logger;
        options1.cacheSize = options.cacheSize;
        options1.allowMmapReads = options.allowMmapReads;
        options1.allowMmapWrites = options.allowMmapWrites;
        options1.filterPolicy = options.filterPolicy;
        return options1;
    }

    static void checkArgNotNull(Object value, String name)
    {
        if (value == null) {
            throw new IllegalArgumentException("The " + name + " argument cannot be null");
        }
    }

    public boolean createIfMissing()
    {
        return createIfMissing;
    }

    public Options createIfMissing(boolean createIfMissing)
    {
        this.createIfMissing = createIfMissing;
        return this;
    }

    public boolean errorIfExists()
    {
        return errorIfExists;
    }

    public Options errorIfExists(boolean errorIfExists)
    {
        this.errorIfExists = errorIfExists;
        return this;
    }

    public int writeBufferSize()
    {
        return writeBufferSize;
    }

    public Options writeBufferSize(int writeBufferSize)
    {
        this.writeBufferSize = writeBufferSize;
        return this;
    }

    public int maxOpenFiles()
    {
        return maxOpenFiles;
    }

    public Options maxOpenFiles(int maxOpenFiles)
    {
        this.maxOpenFiles = maxOpenFiles;
        return this;
    }

    public int blockRestartInterval()
    {
        return blockRestartInterval;
    }

    public Options blockRestartInterval(int blockRestartInterval)
    {
        this.blockRestartInterval = blockRestartInterval;
        return this;
    }

    public int blockSize()
    {
        return blockSize;
    }

    public Options blockSize(int blockSize)
    {
        this.blockSize = blockSize;
        return this;
    }

    public CompressionType compressionType()
    {
        return compressionType;
    }

    public Options compressionType(CompressionType compressionType)
    {
        checkArgNotNull(compressionType, "compressionType");
        this.compressionType = compressionType;
        return this;
    }

    public boolean verifyChecksums()
    {
        return verifyChecksums;
    }

    public Options verifyChecksums(boolean verifyChecksums)
    {
        this.verifyChecksums = verifyChecksums;
        return this;
    }

    public long cacheSize()
    {
        return cacheSize;
    }

    public Options cacheSize(long cacheSize)
    {
        this.cacheSize = cacheSize;
        return this;
    }

    public DBComparator comparator()
    {
        return comparator;
    }

    public Options comparator(DBComparator comparator)
    {
        this.comparator = comparator;
        return this;
    }

    public Logger logger()
    {
        return logger;
    }

    public Options logger(Logger logger)
    {
        this.logger = logger;
        return this;
    }

    public boolean paranoidChecks()
    {
        return paranoidChecks;
    }

    public Options paranoidChecks(boolean paranoidChecks)
    {
        this.paranoidChecks = paranoidChecks;
        return this;
    }

    public Options allowMmapReads(boolean allowMmapReads)
    {
        this.allowMmapReads = allowMmapReads;
        return this;
    }

    public boolean allowMmapReads()
    {
        return allowMmapReads;
    }

    public Options allowMmapWrites(boolean allowMmapWrites)
    {
        this.allowMmapWrites = allowMmapWrites;
        return this;
    }

    public boolean allowMmapWrites()
    {
        return allowMmapWrites;
    }

    /**
     * Set table filter policy
     *
     * @param filterPolicy new filter policy
     * @return self
     */
    public Options filterPolicy(XFilterPolicy filterPolicy)
    {
        this.filterPolicy = filterPolicy;
        return this;
    }

    public XFilterPolicy filterPolicy()
    {
        return filterPolicy;
    }
}
