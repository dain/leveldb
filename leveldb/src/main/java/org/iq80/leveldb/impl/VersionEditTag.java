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

import com.google.common.base.Charsets;
import org.iq80.leveldb.util.VariableLengthQuantity;
import org.jboss.netty.buffer.ChannelBuffer;

import java.util.Map.Entry;

import static org.iq80.leveldb.util.Buffers.readLengthPrefixedBytes;
import static org.iq80.leveldb.util.Buffers.writeLengthPrefixedBytes;

public enum VersionEditTag
{
    COMPARATOR(1)
            {
                @Override
                public void readValue(ChannelBuffer buffer, VersionEdit versionEdit)
                {
                    byte[] bytes = new byte[VariableLengthQuantity.unpackInt(buffer)];
                    buffer.readBytes(bytes);
                    versionEdit.setComparatorName(new String(bytes, Charsets.UTF_8));
                }

                @Override
                public void writeValue(ChannelBuffer buffer, VersionEdit versionEdit)
                {
                    String comparatorName = versionEdit.getComparatorName();
                    if (comparatorName != null) {
                        VariableLengthQuantity.packInt(getPersistentId(), buffer);
                        byte[] bytes = comparatorName.getBytes(Charsets.UTF_8);
                        VariableLengthQuantity.packInt(bytes.length, buffer);
                        buffer.writeBytes(bytes);
                    }
                }
            },
    LOG_NUMBER(2)
            {
                @Override
                public void readValue(ChannelBuffer buffer, VersionEdit versionEdit)
                {
                    versionEdit.setLogNumber(VariableLengthQuantity.unpackLong(buffer));
                }

                @Override
                public void writeValue(ChannelBuffer buffer, VersionEdit versionEdit)
                {
                    Long logNumber = versionEdit.getLogNumber();
                    if (logNumber != null) {
                        VariableLengthQuantity.packInt(getPersistentId(), buffer);
                        VariableLengthQuantity.packLong(logNumber, buffer);
                    }
                }
            },

    NEXT_FILE_NUMBER(3)
            {
                @Override
                public void readValue(ChannelBuffer buffer, VersionEdit versionEdit)
                {
                    versionEdit.setNextFileNumber(VariableLengthQuantity.unpackLong(buffer));
                }

                @Override
                public void writeValue(ChannelBuffer buffer, VersionEdit versionEdit)
                {
                    Long nextFileNumber = versionEdit.getNextFileNumber();
                    if (nextFileNumber != null) {
                        VariableLengthQuantity.packInt(getPersistentId(), buffer);
                        VariableLengthQuantity.packLong(nextFileNumber, buffer);
                    }
                }
            },

    LAST_SEQUENCE(4)
            {
                @Override
                public void readValue(ChannelBuffer buffer, VersionEdit versionEdit)
                {
                    versionEdit.setLastSequenceNumber(VariableLengthQuantity.unpackLong(buffer));
                }

                @Override
                public void writeValue(ChannelBuffer buffer, VersionEdit versionEdit)
                {
                    Long lastSequenceNumber = versionEdit.getLastSequenceNumber();
                    if (lastSequenceNumber != null) {
                        VariableLengthQuantity.packInt(getPersistentId(), buffer);
                        VariableLengthQuantity.packLong(lastSequenceNumber, buffer);
                    }
                }
            },

    COMPACT_POINTER(5)
            {
                @Override
                public void readValue(ChannelBuffer buffer, VersionEdit versionEdit)
                {
                    // level
                    int level = VariableLengthQuantity.unpackInt(buffer);

                    // internal key
                    InternalKey internalKey = new InternalKey(readLengthPrefixedBytes(buffer));

                    versionEdit.setCompactPointer(level, internalKey);
                }

                @Override
                public void writeValue(ChannelBuffer buffer, VersionEdit versionEdit)
                {
                    for (Entry<Integer, InternalKey> entry : versionEdit.getCompactPointers().entrySet()) {
                        VariableLengthQuantity.packInt(getPersistentId(), buffer);

                        // level
                        VariableLengthQuantity.packInt(entry.getKey(), buffer);

                        // internal key
                        writeLengthPrefixedBytes(buffer, entry.getValue().encode());
                    }
                }
            },

    DELETED_FILE(6)
            {
                @Override
                public void readValue(ChannelBuffer buffer, VersionEdit versionEdit)
                {
                    // level
                    int level = VariableLengthQuantity.unpackInt(buffer);

                    // file number
                    long fileNumber = VariableLengthQuantity.unpackLong(buffer);

                    versionEdit.deleteFile(level, fileNumber);
                }

                @Override
                public void writeValue(ChannelBuffer buffer, VersionEdit versionEdit)
                {
                    for (Entry<Integer, Long> entry : versionEdit.getDeletedFiles().entries()) {
                        VariableLengthQuantity.packInt(getPersistentId(), buffer);

                        // level
                        VariableLengthQuantity.packInt(entry.getKey(), buffer);

                        // file number
                        VariableLengthQuantity.packLong(entry.getValue(), buffer);
                    }
                }
            },

    NEW_FILE(7)
            {
                @Override
                public void readValue(ChannelBuffer buffer, VersionEdit versionEdit)
                {
                    // level
                    int level = VariableLengthQuantity.unpackInt(buffer);

                    // file number
                    long fileNumber = VariableLengthQuantity.unpackLong(buffer);

                    // file size
                    long fileSize = VariableLengthQuantity.unpackLong(buffer);

                    // smallest key
                    InternalKey smallestKey = new InternalKey(readLengthPrefixedBytes(buffer));

                    // largest key
                    InternalKey largestKey = new InternalKey(readLengthPrefixedBytes(buffer));

                    versionEdit.addFile(level, fileNumber, fileSize, smallestKey, largestKey);
                }

                @Override
                public void writeValue(ChannelBuffer buffer, VersionEdit versionEdit)
                {
                    for (Entry<Integer, FileMetaData> entry : versionEdit.getNewFiles().entries()) {
                        VariableLengthQuantity.packInt(getPersistentId(), buffer);

                        // level
                        VariableLengthQuantity.packInt(entry.getKey(), buffer);


                        // file number
                        FileMetaData fileMetaData = entry.getValue();
                        VariableLengthQuantity.packLong(fileMetaData.getNumber(), buffer);

                        // file size
                        VariableLengthQuantity.packLong(fileMetaData.getFileSize(), buffer);

                        // smallest key
                        writeLengthPrefixedBytes(buffer, fileMetaData.getSmallest().encode());

                        // smallest key
                        writeLengthPrefixedBytes(buffer, fileMetaData.getLargest().encode());
                    }
                }
            },

    // 8 was used for large value refs

    PREVIOUS_LOG_NUMBER(9)
            {
                @Override
                public void readValue(ChannelBuffer buffer, VersionEdit versionEdit)
                {
                    long previousLogNumber = VariableLengthQuantity.unpackLong(buffer);
                    versionEdit.setPreviousLogNumber(previousLogNumber);
                }

                @Override
                public void writeValue(ChannelBuffer buffer, VersionEdit versionEdit)
                {
                    Long previousLogNumber = versionEdit.getPreviousLogNumber();
                    if (previousLogNumber != null) {
                        VariableLengthQuantity.packInt(getPersistentId(), buffer);
                        VariableLengthQuantity.packLong(previousLogNumber, buffer);
                    }
                }
            },;

    public static VersionEditTag getValueTypeByPersistentId(int persistentId)
    {
        for (VersionEditTag compressionType : VersionEditTag.values()) {
            if (compressionType.persistentId == persistentId) {
                return compressionType;
            }
        }
        throw new IllegalArgumentException(String.format("Unknown %s persistentId %d", VersionEditTag.class.getSimpleName(), persistentId));
    }

    private final int persistentId;

    VersionEditTag(int persistentId)
    {
        this.persistentId = persistentId;
    }

    public int getPersistentId()
    {
        return persistentId;
    }

    public abstract void readValue(ChannelBuffer buffer, VersionEdit versionEdit);

    public abstract void writeValue(ChannelBuffer buffer, VersionEdit versionEdit);
}
