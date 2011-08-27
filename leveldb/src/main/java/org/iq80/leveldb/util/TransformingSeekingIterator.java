package org.iq80.leveldb.util;

import com.google.common.base.Function;
import org.iq80.leveldb.impl.SeekingIterator;

import java.util.Map.Entry;

public class TransformingSeekingIterator<K1, V1, K2, V2> extends AbstractSeekingIterator<K2, V2>
{
    private final SeekingIterator<K1, V1> fromIterator;
    private final Function<Entry<K1, V1>, Entry<K2, V2>> entryFunction;
    private final Function<? super K2, ? extends K1> reverseKeyFunction;

    public TransformingSeekingIterator(SeekingIterator<K1, V1> fromIterator,
            Function<Entry<K1, V1>, Entry<K2, V2>> entryFunction,
            Function<? super K2, ? extends K1> reverseKeyFunction)
    {
        this.fromIterator = fromIterator;
        this.entryFunction = entryFunction;
        this.reverseKeyFunction = reverseKeyFunction;
    }

    @Override
    protected void seekToFirstInternal()
    {
        fromIterator.seekToFirst();
    }

    @Override
    public void seekInternal(K2 targetKey)
    {
        fromIterator.seek(reverseKeyFunction.apply(targetKey));
    }

    @Override
    protected Entry<K2, V2> getNextElement()
    {
        if (fromIterator.hasNext()) {
            Entry<K1, V1> from = fromIterator.next();
            return entryFunction.apply(from);
        }
        return null;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("TransformingSeekingIterator");
        sb.append("{fromIterator=").append(fromIterator);
        sb.append(", entryFunction=").append(entryFunction);
        sb.append(", reverseKeyFunction=").append(reverseKeyFunction);
        sb.append('}');
        return sb.toString();
    }
}
