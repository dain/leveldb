package org.iq80.leveldb.impl;

public class ReadStats
{
    private int seekFileLevel = -1;
    private FileMetaData seekFile;

    public void clear() {
        seekFileLevel = -1;
        seekFile = null;
    }

    public int getSeekFileLevel()
    {
        return seekFileLevel;
    }

    public void setSeekFileLevel(int seekFileLevel)
    {
        this.seekFileLevel = seekFileLevel;
    }

    public FileMetaData getSeekFile()
    {
        return seekFile;
    }

    public void setSeekFile(FileMetaData seekFile)
    {
        this.seekFile = seekFile;
    }
}
