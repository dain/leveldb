package org.iq80.leveldb.impl;

import org.iq80.leveldb.util.PureJavaCrc32C;

import java.io.File;
import java.io.IOException;

public final class Logs
{
    private Logs()
    {
    }

    public static LogWriter createLogWriter(File file, long fileNumber)
            throws IOException
    {
        return new MMapLogWriter(file, fileNumber);
    }

    public static int getChunkChecksum(int chunkTypeId, byte[] buffer, int offset, int length)
    {
        // Compute the crc of the record type and the payload.
        PureJavaCrc32C crc32C = new PureJavaCrc32C();
        crc32C.update(chunkTypeId);
        crc32C.update(buffer, offset, length);
        return crc32C.getMaskedValue();
    }
}
