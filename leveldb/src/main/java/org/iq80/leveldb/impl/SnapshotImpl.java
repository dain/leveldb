package org.iq80.leveldb.impl;

import org.iq80.leveldb.Snapshot;

// todo implement snapshot tracking and cleanup
public class SnapshotImpl implements Snapshot
{
    final long snapshot;

    SnapshotImpl(long snapshot)
    {
        this.snapshot = snapshot;
    }

    @Override
    public void release()
    {
        // todo
//        throw new UnsupportedOperationException();
    }
}
