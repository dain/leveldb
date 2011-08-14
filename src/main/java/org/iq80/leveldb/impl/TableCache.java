package org.iq80.leveldb.impl;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.io.Closeables;
import org.iq80.leveldb.SeekingIterator;
import org.iq80.leveldb.table.Table;
import org.iq80.leveldb.table.UserComparator;
import org.iq80.leveldb.util.SeekingIterators;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutionException;

import static org.iq80.leveldb.impl.InternalKey.CHANNEL_BUFFER_TO_INTERNAL_KEY;
import static org.iq80.leveldb.impl.InternalKey.INTERNAL_KEY_TO_CHANNEL_BUFFER;

public class TableCache
{
    private final Cache<Long, TableAndFile> cache;

    public TableCache(final File databaseDir, int tableCacheSize, final UserComparator channelBufferComparator, final boolean verifyChecksums)
    {
        Preconditions.checkNotNull(databaseDir, "databaseName is null");

        cache = CacheBuilder.newBuilder()
                .maximumSize(tableCacheSize)
                .build(new CacheLoader<Long, TableAndFile>()
                {
                    public TableAndFile load(Long fileNumber)
                            throws IOException
                    {
                        return new TableAndFile(databaseDir, fileNumber, channelBufferComparator, verifyChecksums);
                    }
                });
    }

    public SeekingIterator<InternalKey, ChannelBuffer> newIterator(FileMetaData file)
    {
        return newIterator(file.getNumber());
    }

    public SeekingIterator<InternalKey, ChannelBuffer> newIterator(long number)
    {
        Table table = getTable(number);
        return SeekingIterators.transformKeys(table.iterator(), CHANNEL_BUFFER_TO_INTERNAL_KEY, INTERNAL_KEY_TO_CHANNEL_BUFFER);
    }

    public long getApproximateOffsetOf(FileMetaData file, ChannelBuffer key) {
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

    // todo channel must be closed at some point
    private static final class TableAndFile
    {
        private final Table table;
        private final FileChannel fileChannel;
        private final UserComparator userComparator;

        private TableAndFile(File databaseDir, long fileNumber, UserComparator userComparator, boolean verifyChecksums)
                throws IOException
        {
            this.userComparator = userComparator;
            String tableFileName = Filename.tableFileName(fileNumber);
            File tableFile = new File(databaseDir, tableFileName);
            fileChannel = new FileInputStream(tableFile).getChannel();

            try {
                table = new Table(fileChannel, this.userComparator, verifyChecksums);
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
}
