package org.iq80.leveldb.impl;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class DbLock
{
    private final File lockFile;
    private final FileChannel channel;
    private final FileLock lock;

    public DbLock(File lockFile)
            throws IOException
    {
        Preconditions.checkNotNull(lockFile, "lockFile is null");
        this.lockFile = lockFile;

        // open and lock the file
        channel = new RandomAccessFile(lockFile, "rw").getChannel();
        try {
            lock = channel.tryLock();
        }
        catch (IOException e) {
            Closeables.closeQuietly(channel);
            throw e;
        }
    }

    public boolean isValid()
    {
        return lock.isValid();
    }

    public void release()
    {
        try {
            lock.release();
        }
        catch (IOException e) {
            Throwables.propagate(e);
        }
        finally {
            Closeables.closeQuietly(channel);
        }
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("DbLock");
        sb.append("{lockFile=").append(lockFile);
        sb.append(", lock=").append(lock);
        sb.append('}');
        return sb.toString();
    }
}
