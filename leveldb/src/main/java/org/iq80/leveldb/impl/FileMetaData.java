package org.iq80.leveldb.impl;

import com.google.common.base.Function;

import java.util.concurrent.atomic.AtomicInteger;

public class FileMetaData
{
    public static Function<FileMetaData, InternalKey> GET_LARGEST_USER_KEY = new Function<FileMetaData, InternalKey>()
    {
        @Override
        public InternalKey apply(FileMetaData fileMetaData)
        {
            return fileMetaData.getLargest();
        }
    };

    private final long number;

    /**
     * File size in bytes
     */
    private final long fileSize;

    /**
     * Smallest internal key served by table
     */
    private final InternalKey smallest;

    /**
     * Largest internal key served by table
     */
    private final InternalKey largest;

    /**
     * Seeks allowed until compaction
     */
    // todo this mutable state should be moved elsewhere
    private AtomicInteger allowedSeeks = new AtomicInteger(1 << 30);

    public FileMetaData(long number, long fileSize, InternalKey smallest, InternalKey largest)
    {
        this.number = number;
        this.fileSize = fileSize;
        this.smallest = smallest;
        this.largest = largest;
    }

    public long getFileSize()
    {
        return fileSize;
    }

    public long getNumber()
    {
        return number;
    }

    public InternalKey getSmallest()
    {
        return smallest;
    }

    public InternalKey getLargest()
    {
        return largest;
    }

    public int getAllowedSeeks()
    {
        return allowedSeeks.get();
    }

    public void setAllowedSeeks(int allowedSeeks)
    {
        this.allowedSeeks.set(allowedSeeks);
    }

    public void decrementAllowedSeeks()
    {
        allowedSeeks.getAndDecrement();
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("FileMetaData");
        sb.append("{number=").append(number);
        sb.append(", fileSize=").append(fileSize);
        sb.append(", smallest=").append(smallest);
        sb.append(", largest=").append(largest);
        sb.append(", allowedSeeks=").append(allowedSeeks);
        sb.append('}');
        return sb.toString();
    }
}
