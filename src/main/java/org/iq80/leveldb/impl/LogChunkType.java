package org.iq80.leveldb.impl;

import com.google.common.base.Preconditions;
import org.iq80.leveldb.util.PureJavaCrc32C;

public enum LogChunkType
{
    ZERO_TYPE(0),
    FULL(1),
    FIRST(2),
    MIDDLE(3),
    LAST(4),
    EOF,
    BAD_CHUNK,
    UNKNOWN;

    public static LogChunkType getLogChunkTypeByPersistentId(int persistentId) {
        for (LogChunkType logChunkType : LogChunkType.values()) {
            if (logChunkType.persistentId != null && logChunkType.persistentId == persistentId) {
                return logChunkType;
            }
        }
        return UNKNOWN;
    }

    private final Integer persistentId;

    LogChunkType()
    {
        this.persistentId = null;
    }

    LogChunkType(int persistentId)
    {
        this.persistentId = persistentId;
    }

    public int getPersistentId()
    {
        Preconditions.checkArgument(persistentId != null, "%s is not a persistent chunk type", name());
        return persistentId;
    }
}
