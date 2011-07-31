package org.iq80.leveldb.impl;

public interface LogMonitor
{
    public void corruption(long bytes, String reason);
    public void corruption(long bytes, Throwable reason);
}
