package org.iq80.leveldb.impl;

import org.iq80.leveldb.Snapshot;

public class ReadOptions
{
    private Snapshot snapshot;

    public Snapshot getSnapshot()
    {
        return snapshot;
    }

    public ReadOptions setSnapshot(Snapshot snapshot)
    {
        this.snapshot = snapshot;
        return this;
    }
}
