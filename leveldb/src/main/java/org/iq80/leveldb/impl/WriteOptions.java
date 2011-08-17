package org.iq80.leveldb.impl;

import org.iq80.leveldb.Snapshot;

public class WriteOptions
{
    private boolean sync;
    private Snapshot postWriteSnapshot;


    public boolean isSync()
    {
        return sync;
    }

    public WriteOptions setSync(boolean sync)
    {
        this.sync = sync;
        return this;
    }

    public Snapshot getPostWriteSnapshot()
    {
        return postWriteSnapshot;
    }

    public WriteOptions setPostWriteSnapshot(Snapshot postWriteSnapshot)
    {
        this.postWriteSnapshot = postWriteSnapshot;
        return this;
    }
}
