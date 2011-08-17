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
package org.iq80.leveldb.impl;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.io.Closeables;
import org.iq80.leveldb.table.Table;
import org.iq80.leveldb.table.UserComparator;
import org.iq80.leveldb.util.Finalizer;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.SeekingIterators;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static org.iq80.leveldb.impl.InternalKey.SLICE_TO_INTERNAL_KEY;
import static org.iq80.leveldb.impl.InternalKey.INTERNAL_KEY_TO_SLICE;

public class TableCache
{
    private final Cache<Long, TableAndFile> cache;
    private final Finalizer<Table> finalizer = new Finalizer<Table>(1);

    public TableCache(final File databaseDir, int tableCacheSize, final UserComparator userComparator, final boolean verifyChecksums)
    {
        Preconditions.checkNotNull(databaseDir, "databaseName is null");

        cache = CacheBuilder.newBuilder()
                .maximumSize(tableCacheSize)
                .removalListener(new RemovalListener<Long, TableAndFile>()
                {
                    public void onRemoval(RemovalNotification<Long, TableAndFile> notification)
                    {
                        TableAndFile tableAndFile = notification.getValue();
                        finalizer.addCleanup(tableAndFile.getTable(), new CloseQuietly(tableAndFile.fileChannel));
                    }
                })
                .build(new CacheLoader<Long, TableAndFile>()
                {
                    public TableAndFile load(Long fileNumber)
                            throws IOException
                    {
                        return new TableAndFile(databaseDir, fileNumber, userComparator, verifyChecksums);
                    }
                });
    }

    public SeekingIterator<InternalKey, Slice> newIterator(FileMetaData file)
    {
        return newIterator(file.getNumber());
    }

    public SeekingIterator<InternalKey, Slice> newIterator(long number)
    {
        Table table = getTable(number);
        return SeekingIterators.transformKeys(table.iterator(), SLICE_TO_INTERNAL_KEY, INTERNAL_KEY_TO_SLICE);
    }

    public long getApproximateOffsetOf(FileMetaData file, Slice key) {
        return getTable(file.getNumber()).getApproximateOffsetOf(key);
    }

    private Table getTable(long number)
    {
        Table table;
        try {
            table = cache.get(number).getTable();
        }
        catch (ExecutionException e) {
            Throwable cause = e;
            if (e.getCause() != null) {
                cause = e.getCause();
            }
            throw new RuntimeException("Could not open table " + number, cause);
        }
        return table;
    }

    public void evict(long number)
    {
        cache.invalidate(number);
    }

    private static final class TableAndFile
    {
        private final Table table;
        private final FileChannel fileChannel;

        private TableAndFile(File databaseDir, long fileNumber, UserComparator userComparator, boolean verifyChecksums)
                throws IOException
        {
            String tableFileName = Filename.tableFileName(fileNumber);
            File tableFile = new File(databaseDir, tableFileName);
            fileChannel = new FileInputStream(tableFile).getChannel();

            try {
                table = new Table(tableFile.getAbsolutePath(), fileChannel, userComparator, verifyChecksums);
            }
            catch (IOException e) {
                Closeables.closeQuietly(fileChannel);
                throw e;
            }
        }

        public Table getTable()
        {
            return table;
        }
    }

    private static class CloseQuietly implements Callable<Void>
    {
        private final Closeable closeable;

        public CloseQuietly(Closeable closeable)
        {
            this.closeable = closeable;
        }

        public Void call()
        {
            Closeables.closeQuietly(closeable);
            return null;
        }
    }
}
