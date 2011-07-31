package org.iq80.leveldb.impl;

import org.iq80.leveldb.util.SizeOf;

import static org.iq80.leveldb.util.SizeOf.SIZE_OF_BYTE;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_SHORT;

public final class LogConstants
{
    // todo find new home for these

    public static final int BLOCK_SIZE = 32768;

    // Header is checksum (4 bytes), type (1 byte), length (2 bytes).
    public static final int HEADER_SIZE = SIZE_OF_INT + SIZE_OF_BYTE + SIZE_OF_SHORT;

}
