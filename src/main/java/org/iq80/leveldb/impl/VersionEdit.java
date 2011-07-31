package org.iq80.leveldb.impl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.iq80.leveldb.util.VariableLengthQuantity;
import org.jboss.netty.buffer.ChannelBuffer;

public class VersionEdit
{
    private String comparatorName;
    private Long logNumber;
    private Long nextFileNumber;
    private Long previousLogNumber;
    private Long lastSequenceNumber;
    private final Multimap<Integer, InternalKey> compactPointers = ArrayListMultimap.create();
    private final Multimap<Integer, FileMetaData> newFiles = ArrayListMultimap.create();
    private final Multimap<Integer, Long> deletedFiles = ArrayListMultimap.create();

    public VersionEdit()
    {
    }

    public VersionEdit(ChannelBuffer buffer)
    {
        while(buffer.readable()) {
            int i = VariableLengthQuantity.unpackInt(buffer);
            VersionEditTag tag = VersionEditTag.getValueTypeByPersistentId(i);
            tag.readValue(buffer, this);
        }
    }

    public String getComparatorName()
    {
        return comparatorName;
    }

    public void setComparatorName(String comparatorName)
    {
        this.comparatorName = comparatorName;
    }

    public Long getLogNumber()
    {
        return logNumber;
    }

    public void setLogNumber(long logNumber)
    {
        this.logNumber = logNumber;
    }

    public Long getNextFileNumber()
    {
        return nextFileNumber;
    }

    public void setNextFileNumber(long nextFileNumber)
    {
        this.nextFileNumber = nextFileNumber;
    }

    public Long getPreviousLogNumber()
    {
        return previousLogNumber;
    }

    public void setPreviousLogNumber(long previousLogNumber)
    {
        this.previousLogNumber = previousLogNumber;
    }

    public Long getLastSequenceNumber()
    {
        return lastSequenceNumber;
    }

    public void setLastSequenceNumber(long lastSequenceNumber)
    {
        this.lastSequenceNumber = lastSequenceNumber;
    }

    public Multimap<Integer, InternalKey> getCompactPointers()
    {
        return ImmutableMultimap.copyOf(compactPointers);
    }

    public void setCompactPointer(int level, InternalKey key)
    {
        compactPointers.put(level, key);
    }

    public void setCompactPointers(Multimap<Integer, InternalKey> compactPointers)
    {
        this.compactPointers.putAll(compactPointers);
    }

    public Multimap<Integer, FileMetaData> getNewFiles()
    {
        return ImmutableMultimap.copyOf(newFiles);
    }

    // Add the specified file at the specified level.
    // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
    // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
    public void addFile(int level, long fileNumber,
            long fileSize,
            InternalKey smallest,
            InternalKey largest)
    {

        FileMetaData fileMetaData = new FileMetaData(fileNumber, fileSize, smallest, largest);
        addFile(level, fileMetaData);
    }

    public void addFile(int level, FileMetaData fileMetaData)
    {
        newFiles.put(level, fileMetaData);
    }

    public void addFiles(Multimap<Integer, FileMetaData> files)
    {
        newFiles.putAll(files);
    }


    public Multimap<Integer, Long> getDeletedFiles()
    {
        return ImmutableMultimap.copyOf(deletedFiles);
    }

    // Delete the specified "file" from the specified "level".
    public void deleteFile(int level, long fileNumber)
    {
        deletedFiles.put(level, fileNumber);
    }

    public void encodeTo(ChannelBuffer buffer)
    {
        for (VersionEditTag versionEditTag : VersionEditTag.values()) {
            versionEditTag.writeValue(buffer, this);
        }
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("VersionEdit");
        sb.append("{comparatorName='").append(comparatorName).append('\'');
        sb.append(", logNumber=").append(logNumber);
        sb.append(", previousLogNumber=").append(previousLogNumber);
        sb.append(", lastSequenceNumber=").append(lastSequenceNumber);
        sb.append(", compactPointers=").append(compactPointers);
        sb.append(", newFiles=").append(newFiles);
        sb.append(", deletedFiles=").append(deletedFiles);
        sb.append('}');
        return sb.toString();
    }
}
