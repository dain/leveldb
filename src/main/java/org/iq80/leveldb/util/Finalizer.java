package org.iq80.leveldb.util;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class Finalizer<T>
{
    public static final FinalizerMonitor IGNORE_FINALIZER_MONITOR = new FinalizerMonitor()
    {
        public void unexpectedException(Throwable throwable)
        {
        }
    };

    private final int threads;
    private final FinalizerMonitor monitor;

    private final Set<FinalizerPhantomReference<T>> references = new HashSet<FinalizerPhantomReference<T>>();
    private final ReferenceQueue<T> referenceQueue = new ReferenceQueue<T>();
    private boolean destroyed;
    private ExecutorService executor;

    public Finalizer()
    {
        this(1, IGNORE_FINALIZER_MONITOR);
    }

    public Finalizer(int threads)
    {
        this(1, IGNORE_FINALIZER_MONITOR);
    }

    public Finalizer(int threads, FinalizerMonitor monitor)
    {
        this.monitor = monitor;
        Preconditions.checkArgument(threads >= 1, "threads must be at least 1");
        this.threads = threads;
    }

    public synchronized void addCleanup(T item, Callable<?> cleanup)
    {
        Preconditions.checkNotNull(item, "item is null");
        Preconditions.checkNotNull(cleanup, "cleanup is null");
        Preconditions.checkState(!destroyed, "%s is destroyed", getClass().getName());

        if (executor == null) {
            // create executor
            ThreadFactory threadFactory = new ThreadFactoryBuilder()
                    .setNameFormat("FinalizerQueueProcessor-%d")
                    .setDaemon(true)
                    .build();
            executor = Executors.newFixedThreadPool(threads, threadFactory);

            // start queue processor jobs
            for (int i = 0; i < threads; i++) {
                executor.submit(new FinalizerQueueProcessor());
            }
        }

        // create a reference to the item so we are notified when it is garbage collected
        FinalizerPhantomReference<T> reference = new FinalizerPhantomReference<T>(item, referenceQueue, cleanup);

        // we must keep a strong reference to the reference object so we are notified when the item
        // is no longer reachable (if the reference object is garbage collected we are never notified)
        references.add(reference);
    }

    public synchronized void destroy()
    {
        destroyed = true;
        executor.shutdownNow();
    }

    public interface FinalizerMonitor
    {
        void unexpectedException(Throwable throwable);
    }

    private static class FinalizerPhantomReference<T> extends PhantomReference<T>
    {
        private final Callable<?> cleanup;

        private FinalizerPhantomReference(T referent, ReferenceQueue<? super T> queue, Callable<?> cleanup)
        {
            super(referent, queue);
            this.cleanup = cleanup;
        }
    }

    private class FinalizerQueueProcessor implements Runnable
    {
        @Override
        public void run()
        {
            while (!destroyed) {
                // get the next reference to cleanup
                FinalizerPhantomReference<T> reference;
                try {
                    reference = (FinalizerPhantomReference<T>) referenceQueue.remove();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

                // remove the reference object itself from our list of references
                references.remove(reference);

                boolean rescheduleAndReturn = false;
                try {
                    reference.cleanup.call();
                    rescheduleAndReturn = Thread.currentThread().isInterrupted();
                }
                catch (Throwable userException) {
                    try {
                        monitor.unexpectedException(userException);
                    }
                    catch (Exception ignored) {
                        // todo consider a broader notification
                    }

                    if (userException instanceof InterruptedException) {
                        rescheduleAndReturn = true;
                        Thread.currentThread().interrupt();
                    }
                    else if (userException instanceof Error) {
                        rescheduleAndReturn = true;
                    }
                }

                if (rescheduleAndReturn) {
                    synchronized (Finalizer.this) {
                        if (!destroyed) {
                            executor.submit(new FinalizerQueueProcessor());
                        }
                    }
                    return;
                }
            }
        }
    }
}
