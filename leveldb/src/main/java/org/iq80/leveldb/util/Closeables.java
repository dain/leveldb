package org.iq80.leveldb.util;

import java.io.Closeable;
import java.io.IOException;

public final class Closeables
{
    private Closeables() {}

    public static void closeQuietly(Closeable closeable)
    {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        }
        catch (IOException ignored) {
        }
    }
}
