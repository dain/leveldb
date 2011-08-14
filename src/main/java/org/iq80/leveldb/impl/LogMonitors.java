package org.iq80.leveldb.impl;

public final class LogMonitors
{
    public static LogMonitor throwExceptionMonitor() {
        return new LogMonitor()
        {
            @Override
            public void corruption(long bytes, String reason)
            {
                throw new RuntimeException(String.format("corruption of %s bytes: %s", bytes, reason));
            }

            @Override
            public void corruption(long bytes, Throwable reason)
            {
                throw new RuntimeException(String.format("corruption of %s bytes", bytes), reason);
            }
        };
    }

    // todo implement real logging
    public static LogMonitor logMonitor() {
        return new LogMonitor()
        {
            @Override
            public void corruption(long bytes, String reason)
            {
                System.out.print(String.format("corruption of %s bytes: %s", bytes, reason));
            }

            @Override
            public void corruption(long bytes, Throwable reason)
            {
                System.out.print(String.format("corruption of %s bytes", bytes));
                reason.printStackTrace();
            }
        };
    }

    private LogMonitors()
    {
    }
}
