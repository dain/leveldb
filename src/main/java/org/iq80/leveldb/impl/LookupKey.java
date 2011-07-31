package org.iq80.leveldb.impl;

import org.iq80.leveldb.util.VariableLengthQuantity;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import static org.iq80.leveldb.util.SizeOf.SIZE_OF_LONG;

public class LookupKey
{
    // We construct a buffer of the form:
    //    klength  varint32               <-- start_
    //    userkey  char[klength]          <-- kstart_
    //    tag      uint64
    //                                    <-- end_
    // The array is a suitable MemTable key.
    // The suffix starting with "userkey" can be used as an InternalKey.

    private final ChannelBuffer key;
    private final int keyStart;

    public LookupKey(ChannelBuffer user_key, long sequenceNumber)
    {
        // A conservative estimate of the key length
        // todo add function to calculate exact size of packed int
        key = ChannelBuffers.buffer(user_key.readableBytes() + 13);

        // write length
        VariableLengthQuantity.packInt(user_key.readableBytes() + 8, key);
        keyStart = key.readableBytes();

        // write bytes
        key.writeBytes(user_key, 0, user_key.readableBytes());

        // write sequence number
        key.writeLong(SequenceNumber.packSequenceAndValueType(sequenceNumber, ValueType.VALUE));
    }

    public ChannelBuffer getMemtableKey()
    {
        // full key
        return key.duplicate();
    }

    public InternalKey getInternalKey()
    {
        // user key + tag
        return new InternalKey(key.slice(keyStart, key.readableBytes() - keyStart));
    }

    public ChannelBuffer getUserKey()
    {
        // just user key part -- no key length and no tag
        return key.slice(keyStart, key.readableBytes() - keyStart - SIZE_OF_LONG);
    }

    @Override
    public String toString()
    {
        return getInternalKey().toString();
    }
}
