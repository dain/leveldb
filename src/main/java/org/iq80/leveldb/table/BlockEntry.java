package org.iq80.leveldb.table;

import com.google.common.base.Preconditions;
import org.jboss.netty.buffer.ChannelBuffer;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Binary Structure
 * <table summary="record format">
 * <tbody>
 * <thead>
 * <tr>
 * <th>name</th>
 * <th>offset</th>
 * <th>length</th>
 * <th>description</th>
 * </tr>
 * </thead>
 * <p/>
 * <tr>
 * <td>shared key length</td>
 * <td>0</td>
 * <td>vary</td>
 * <td>variable length encoded int: size of shared key prefix with the key from the previous entry</td>
 * </tr>
 * <tr>
 * <td>non-shared key length</td>
 * <td>vary</td>
 * <td>vary</td>
 * <td>variable length encoded int: size of non-shared key suffix in this entry</td>
 * </tr>
 * <tr>
 * <td>value length</td>
 * <td>vary</td>
 * <td>vary</td>
 * <td>variable length encoded int: size of value in this entry</td>
 * </tr>
 * <tr>
 * <td>non-shared key</td>
 * <td>vary</td>
 * <td>non-shared key length</td>
 * <td>non-shared key data</td>
 * </tr>
 * <tr>
 * <td>value</td>
 * <td>vary</td>
 * <td>value length</td>
 * <td>value data</td>
 * </tr>
 * </tbody>
 * </table>
 */
public class BlockEntry
{
    private final ChannelBuffer key;
    private final ChannelBuffer value;

    public BlockEntry(ChannelBuffer key, ChannelBuffer value)
    {
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(value, "value is null");
        this.key = key;
        this.value = value;
    }

    public ChannelBuffer getKey()
    {
        return key.duplicate();
    }

    public ChannelBuffer getValue()
    {
        return value.duplicate();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BlockEntry entry = (BlockEntry) o;

        if (!key.equals(entry.key)) {
            return false;
        }
        if (!value.equals(entry.value)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = key.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("BlockEntry");
        sb.append("{key=").append(new String(key.copy().array(), UTF_8));      // todo don't print the real value
        sb.append(", value=").append(new String(value.copy().array(), UTF_8));
        sb.append('}');
        return sb.toString();
    }
}
