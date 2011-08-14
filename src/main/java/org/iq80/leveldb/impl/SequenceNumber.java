package org.iq80.leveldb.impl;

import com.google.common.base.Preconditions;

public final class SequenceNumber
{
    // We leave eight bits empty at the bottom so a type and sequence#
    // can be packed together into 64-bits.
    public static final long MAX_SEQUENCE_NUMBER = ((0x1L << 56) - 1);

    public static long packSequenceAndValueType(long sequence, ValueType valueType)
    {
        Preconditions.checkArgument(sequence <= MAX_SEQUENCE_NUMBER, "Sequence number is greater than MAX_SEQUENCE_NUMBER");
        Preconditions.checkNotNull(valueType, "valueType is null");

        return (sequence << 8) | valueType.getPersistentId();
    }

    public static ValueType unpackValueType(long num)
    {
        return ValueType.getValueTypeByPersistentId((byte) num);
    }

    public static long unpackSequenceNumber(long num)
    {
        return num >>> 8;
    }
}
