package org.iq80.leveldb.table;

public class Options
{
    private int blockRestartInterval = 1;
    private int blockSize = 4 * 1024;
    private CompressionType compressionType = CompressionType.SNAPPY;
    private boolean verifyChecksums = true;

    public int getBlockRestartInterval()
    {
        return blockRestartInterval;
    }

    public Options setBlockRestartInterval(int blockRestartInterval)
    {
        this.blockRestartInterval = blockRestartInterval;
        return this;
    }

    public int getBlockSize()
    {
        return blockSize;
    }

    public Options setBlockSize(int blockSize)
    {
        this.blockSize = blockSize;
        return this;
    }

    public CompressionType getCompressionType()
    {
        return compressionType;
    }

    public Options setCompressionType(CompressionType compressionType)
    {
        this.compressionType = compressionType;
        return this;
    }

    public boolean isVerifyChecksums()
    {
        return verifyChecksums;
    }

    public Options setVerifyChecksums(boolean verifyChecksums)
    {
        this.verifyChecksums = verifyChecksums;
        return this;
    }
}
