package org.iq80.leveldb.impl;

public enum ValueType
{
    DELETION(0x00),
    VALUE(0x01);

    public static ValueType getValueTypeByPersistentId(int persistentId) {
        for (ValueType compressionType : ValueType.values()) {
            if (compressionType.persistentId == persistentId) {
                return compressionType;
            }
        }
        throw new IllegalArgumentException("Unknown persistentId " + persistentId);
    }

    private final int persistentId;

    ValueType(int persistentId)
    {
        this.persistentId = persistentId;
    }

    public int getPersistentId()
    {
        return persistentId;
    }
}
