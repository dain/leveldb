package org.iq80.leveldb.impl;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import static com.google.common.base.Charsets.UTF_8;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_LONG;

public class InternalKey
{
    public InternalKey(ChannelBuffer userKey, long sequenceNumber, ValueType valueType)
    {
        Preconditions.checkNotNull(userKey, "userKey is null");
        Preconditions.checkArgument(sequenceNumber >= 0, "sequenceNumber is negative");
        Preconditions.checkNotNull(valueType, "valueType is null");

        ChannelBuffer buffer = ChannelBuffers.buffer(userKey.readableBytes() + SIZE_OF_LONG);
        buffer.writeBytes(userKey.slice());
        buffer.writeLong(SequenceNumber.packSequenceAndValueType(sequenceNumber, valueType));
        data = ChannelBuffers.unmodifiableBuffer(buffer);
    }

    public InternalKey(ChannelBuffer data)
    {
        Preconditions.checkNotNull(data, "data is null");
        if (data.readableBytes() < SIZE_OF_LONG) {
            Preconditions.checkArgument(data.readableBytes() >= SIZE_OF_LONG, "data must be at least %s bytes", SIZE_OF_LONG);
        }
        this.data = data;
    }

    public ChannelBuffer getUserKey()
    {
        ChannelBuffer buffer = data.duplicate();
        buffer.writerIndex(data.readableBytes() - SIZE_OF_LONG);
        return buffer;
    }

    public long getSequenceNumber()
    {
        return SequenceNumber.unpackSequenceNumber(data.getLong(data.readableBytes() - SIZE_OF_LONG));
    }

    public ValueType getValueType()
    {
        return SequenceNumber.unpackValueType(data.getLong(data.readableBytes() - SIZE_OF_LONG));
    }

    public ChannelBuffer encode()
    {
        Preconditions.checkState(data.readable());
        return data.duplicate();
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("InternalKey");
        sb.append("{key=").append(getUserKey().toString(UTF_8));      // todo don't print the real value
        sb.append(", sequenceNumber=").append(getSequenceNumber());
        sb.append(", valueType=").append(getValueType());
        sb.append('}');
        return sb.toString();
    }

    // todo find new home for these

    public static final Function<InternalKey, ChannelBuffer> INTERNAL_KEY_TO_CHANNEL_BUFFER = new InternalKeyToChannelBufferFunction();

    public static final Function<ChannelBuffer, InternalKey> CHANNEL_BUFFER_TO_INTERNAL_KEY = new ChannelBufferToInternalKeyFunction();

    public static final Function<InternalKey, ChannelBuffer> INTERNAL_KEY_TO_USER_KEY = new InternalKeyToUserKeyFunction();

    public static Function<ChannelBuffer, InternalKey> createUserKeyToInternalKeyFunction(final long sequenceNumber)
    {
        return new UserKeyInternalKeyFunction(sequenceNumber);
    }

    private final ChannelBuffer data;


    private static class InternalKeyToChannelBufferFunction implements Function<InternalKey, ChannelBuffer>
    {
        @Override
        public ChannelBuffer apply(InternalKey internalKey)
        {
            return internalKey.encode();
        }
    }

    private static class InternalKeyToUserKeyFunction implements Function<InternalKey, ChannelBuffer>
    {
        @Override
        public ChannelBuffer apply(InternalKey internalKey)
        {
            return internalKey.getUserKey();
        }
    }

    private static class ChannelBufferToInternalKeyFunction implements Function<ChannelBuffer, InternalKey>
    {
        @Override
        public InternalKey apply(ChannelBuffer channelBuffer)
        {
            return new InternalKey(channelBuffer);
        }
    }

    private static class UserKeyInternalKeyFunction implements Function<ChannelBuffer, InternalKey>
    {
        private final long sequenceNumber;

        public UserKeyInternalKeyFunction(long sequenceNumber)
        {
            this.sequenceNumber = sequenceNumber;
        }

        @Override
        public InternalKey apply(ChannelBuffer channelBuffer)
        {
            return new InternalKey(channelBuffer, sequenceNumber, ValueType.VALUE);
        }
    }
}
