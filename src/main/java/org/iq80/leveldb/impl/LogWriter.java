package org.iq80.leveldb.impl;

import org.jboss.netty.buffer.ChannelBuffer;

import java.io.File;
import java.io.IOException;

public interface LogWriter
{
    boolean isClosed();

    void close()
            throws IOException;

    void delete()
            throws IOException;

    File getFile();

    long getFileNumber();

    // Writes a stream of chunks such that no chunk is split across a block boundary
    void addRecord(ChannelBuffer record, boolean force)
            throws IOException;
}
