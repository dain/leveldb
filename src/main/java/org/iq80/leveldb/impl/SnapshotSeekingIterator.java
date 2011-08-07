package org.iq80.leveldb.impl;

import org.iq80.leveldb.SeekingIterator;
import org.jboss.netty.buffer.ChannelBuffer;

import java.util.Comparator;
import java.util.Map.Entry;

public class SnapshotSeekingIterator implements SeekingIterator<InternalKey, ChannelBuffer>
{
    private final Comparator<ChannelBuffer> userComparator;
    private final SeekingIterator<InternalKey, ChannelBuffer> iterator;
    private final long snapshot;

    public SnapshotSeekingIterator(SeekingIterator<InternalKey, ChannelBuffer> iterator, long snapshot, Comparator<ChannelBuffer> userComparator)
    {
        this.iterator = iterator;
        this.snapshot = snapshot;
        this.userComparator = userComparator;
    }

    @Override
    public void seekToFirst()
    {
        iterator.seekToFirst();
        findNextUserEntry(null);
    }

    @Override
    public void seek(InternalKey targetKey)
    {
        iterator.seek(targetKey);
        findNextUserEntry(null);
    }

    @Override
    public boolean hasNext()
    {
        return iterator.hasNext();
    }

    @Override
    public Entry<InternalKey, ChannelBuffer> peek()
    {
        return iterator.peek();
    }

    @Override
    public Entry<InternalKey, ChannelBuffer> next()
    {
        Entry<InternalKey, ChannelBuffer> next = iterator.next();

        // find the next user entry after the key we are about to return
        findNextUserEntry(next.getKey().getUserKey());

        return next;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    private void findNextUserEntry(ChannelBuffer deletedKey)
    {
        // if there are no more entries, we are done
        if (!iterator.hasNext()) {
            return;
        }

        do {
            // Peek the next entry and parse the key
            InternalKey internalKey = iterator.peek().getKey();

            // skip entries created after our snapshot
            if (internalKey.getSequenceNumber() > snapshot) {
                iterator.next();
                continue;
            }

            // if the next entry is a deletion, skip all subsequent entries for that key
            if (internalKey.getValueType() == ValueType.DELETION) {
                deletedKey = internalKey.getUserKey();
            }
            else if (internalKey.getValueType() == ValueType.VALUE) {
                // is this value masked by a prior deletion record?
                if (deletedKey == null || userComparator.compare(internalKey.getUserKey(), deletedKey) > 0) {
                    return;
                }
            }
            iterator.next();
        } while (iterator.hasNext());
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("SnapshotSeekingIterator");
        sb.append("{snapshot=").append(snapshot);
        sb.append(", iterator=").append(iterator);
        sb.append('}');
        return sb.toString();
    }
}
