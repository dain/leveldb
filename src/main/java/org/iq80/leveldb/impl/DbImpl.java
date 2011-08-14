package org.iq80.leveldb.impl;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.iq80.leveldb.SeekingIterable;
import org.iq80.leveldb.SeekingIterator;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.impl.Filename.FileInfo;
import org.iq80.leveldb.impl.Filename.FileType;
import org.iq80.leveldb.impl.WriteBatch.Handler;
import org.iq80.leveldb.table.BasicUserComparator;
import org.iq80.leveldb.table.Options;
import org.iq80.leveldb.table.TableBuilder;
import org.iq80.leveldb.util.SeekingIterators;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.collect.Lists.newArrayList;
import static org.iq80.leveldb.impl.DbConstants.L0_SLOWDOWN_WRITES_TRIGGER;
import static org.iq80.leveldb.impl.DbConstants.L0_STOP_WRITES_TRIGGER;
import static org.iq80.leveldb.impl.DbConstants.MAX_MEM_COMPACT_LEVEL;
import static org.iq80.leveldb.impl.DbConstants.NUM_LEVELS;
import static org.iq80.leveldb.impl.InternalKey.INTERNAL_KEY_TO_USER_KEY;
import static org.iq80.leveldb.impl.InternalKey.createUserKeyToInternalKeyFunction;
import static org.iq80.leveldb.impl.SequenceNumber.MAX_SEQUENCE_NUMBER;
import static org.iq80.leveldb.impl.ValueType.DELETION;
import static org.iq80.leveldb.impl.ValueType.VALUE;
import static org.iq80.leveldb.util.Buffers.writeLengthPrefixedBytes;

// todo needs a close method
// todo implement remaining compaction methods
// todo make thread safe and concurrent
public class DbImpl implements SeekingIterable<ChannelBuffer, ChannelBuffer>
{
    private final Options options;
    private final File databaseDir;
    private final TableCache tableCache;
    private final DbLock dbLock;
    private final VersionSet versions;

    private final AtomicBoolean shuttingDown = new AtomicBoolean();
    private final ReentrantLock mutex = new ReentrantLock();
    private final Condition backgroundCondition = mutex.newCondition();

    private final List<Long> pendingOutputs = newArrayList(); // todo

    private FileChannel logChannel;
    private long logFileNumber;
    private LogWriter log;

    private MemTable memTable;
    private MemTable immutableMemTable;

    private final InternalKeyComparator internalKeyComparator;

    private ExecutorService compactionExecutor;
    private Future<?> backgroundCompaction;

    private ManualCompaction manualCompaction;

    public DbImpl(Options options, File databaseDir)
            throws IOException
    {
        Preconditions.checkNotNull(options, "options is null");
        Preconditions.checkNotNull(databaseDir, "databaseDir is null");
        this.options = options;
        this.databaseDir = databaseDir;

        internalKeyComparator = new InternalKeyComparator(new BasicUserComparator());
        memTable = new MemTable(internalKeyComparator);
        immutableMemTable = null;

        ThreadFactory compactionThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("leveldb-compaction-%s")
                .setUncaughtExceptionHandler(new UncaughtExceptionHandler()
                {
                    @Override
                    public void uncaughtException(Thread t, Throwable e)
                    {
                        // todo need a real UncaughtExceptionHandler
                        System.out.printf("%s%n", t);
                        e.printStackTrace();
                    }
                })
                .build();
        compactionExecutor = Executors.newCachedThreadPool(compactionThreadFactory);

        // Reserve ten files or so for other uses and give the rest to TableCache.
        int tableCacheSize = options.getMaxOpenFiles() - 10;
        tableCache = new TableCache(databaseDir, tableCacheSize, new InternalUserComparator(internalKeyComparator), options.isVerifyChecksums());

        // create the version set
        versions = new VersionSet(options, databaseDir, tableCache, internalKeyComparator);

        // create the database dir if it does not already exist
        databaseDir.mkdirs();
        Preconditions.checkArgument(databaseDir.isDirectory(), "Database directory '%s' is not a directory");

        mutex.lock();
        try {
            // lock the database dir
            dbLock = new DbLock(new File(databaseDir, Filename.lockFileName()));

            // verify the "current" file
            File currentFile = new File(databaseDir, Filename.currentFileName());
            if (!currentFile.canRead()) {
                Preconditions.checkArgument(options.isCreateIfMissing(), "Database '%s' does not exist and the create if missing option is disabled", databaseDir);
            }
            else {
                Preconditions.checkArgument(!options.isErrorIfExists(), "Database '%s' exists and the error if exists option is enabled", databaseDir);
            }

            // load  (and recover) current version
            versions.recover();

            // Recover from all newer log files than the ones named in the
            // descriptor (new log files may have been added by the previous
            // incarnation without registering them in the descriptor).
            //
            // Note that PrevLogNumber() is no longer used, but we pay
            // attention to it in case we are recovering a database
            // produced by an older version of leveldb.
            long minLogNumber = versions.getLogNumber();
            long previousLogNumber = versions.getPrevLogNumber();
            List<File> filenames = Filename.listFiles(databaseDir);

            List<Long> logs = Lists.newArrayList();
            for (File filename : filenames) {
                FileInfo fileInfo = Filename.parseFileName(filename);

                if (fileInfo != null &&
                        fileInfo.getFileType() == FileType.LOG &&
                        ((fileInfo.getFileNumber() >= minLogNumber) || (fileInfo.getFileNumber() == previousLogNumber))) {
                    logs.add(fileInfo.getFileNumber());
                }
            }

            // Recover in the order in which the logs were generated
            VersionEdit edit = new VersionEdit();
            Collections.sort(logs);
            for (Long fileNumber : logs) {
//                long maxSequence = recoverLogFile(fileNumber, edit);
//                if (versions.getLastSequence() < maxSequence) {
//                    versions.setLastSequence(maxSequence);
//                }
            }

            // open transaction log
            long newLogNumber = versions.getNextFileNumber();
            File logFile = new File(databaseDir, Filename.logFileName(newLogNumber));
            FileChannel logChannel = new FileOutputStream(logFile).getChannel();
            edit.setLogNumber(newLogNumber);
            this.logChannel = logChannel;
            this.logFileNumber = newLogNumber;
            this.log = new LogWriter(logChannel);

            // apply recovered edits
            versions.logAndApply(edit);

            // cleanup unused files
            deleteObsoleteFiles();

            // schedule compactions
            maybeScheduleCompaction();
        }
        finally {
            mutex.unlock();
        }
    }

    public void close() {
        if (shuttingDown.getAndSet(true)) {
            return;
        }

        mutex.lock();
        try {
            makeRoomForWrite(true);
            while (backgroundCompaction != null) {
                backgroundCondition.awaitUninterruptibly();
            }
        } finally {
            mutex.unlock();
        }

        compactionExecutor.shutdown();
        try {
            compactionExecutor.awaitTermination(1, TimeUnit.DAYS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        dbLock.release();
    }

    private void deleteObsoleteFiles()
    {
        // Make a set of all of the live files
        List<Long> live = newArrayList(this.pendingOutputs);
        for (FileMetaData fileMetaData : versions.getLiveFiles()) {
            live.add(fileMetaData.getNumber());
        }

        for (File file : Filename.listFiles(databaseDir)) {
            FileInfo fileInfo = Filename.parseFileName(file);
            long number = fileInfo.getFileNumber();
            boolean keep = true;
            switch (fileInfo.getFileType()) {
                case LOG:
                    keep = ((number >= versions.getLogNumber()) ||
                            (number == versions.getPrevLogNumber()));
                    break;
                case DESCRIPTOR:
                    // Keep my manifest file, and any newer incarnations'
                    // (in case there is a race that allows other incarnations)
                    keep = (number >= versions.getManifestFileNumber());
                    break;
                case TABLE:
                    keep = live.contains(number);
                    break;
                case TEMP:
                    // Any temp files that are currently being written to must
                    // be recorded in pending_outputs_, which is inserted into "live"
                    keep = live.contains(number);
                    break;
                case CURRENT:
                case DB_LOCK:
                case INFO_LOG:
                    keep = true;
                    break;
            }

            if (!keep) {
                if (fileInfo.getFileType() == FileType.TABLE) {
                    tableCache.evict(number);
                }
                // todo info logging system needed
//                Log(options_.info_log, "Delete type=%d #%lld\n",
//                int(type),
//                        static_cast < unsigned long long>(number));
                file.delete();
            }
        }
    }
    public void flushMemTable()
    {
        mutex.lock();
        try {
            // force compaction
            makeRoomForWrite(true);

            // todo bg_error code
            while(immutableMemTable != null) {
                backgroundCondition.awaitUninterruptibly();
            }

        } finally {
            mutex.unlock();
        }
    }

    public void compactRange(int level, ChannelBuffer start, ChannelBuffer end)
    {
        Preconditions.checkArgument(level >= 0, "level is negative");
        Preconditions.checkArgument(level + 1 < NUM_LEVELS, "level is greater than %s", NUM_LEVELS);
        Preconditions.checkNotNull(start, "start is null");
        Preconditions.checkNotNull(end, "end is null");

        mutex.lock();
        try {
            while (this.manualCompaction != null) {
                backgroundCondition.awaitUninterruptibly();
            }
            ManualCompaction manualCompaction = new ManualCompaction(level, start, end);
            this.manualCompaction = manualCompaction;

            maybeScheduleCompaction();

            while (this.manualCompaction == manualCompaction) {
                backgroundCondition.awaitUninterruptibly();
            }
        }
        finally {
            mutex.unlock();
        }

    }

    private void maybeScheduleCompaction()
    {
        Preconditions.checkState(mutex.isHeldByCurrentThread());

        if (backgroundCompaction != null) {
            // Already scheduled
        }
        else if (shuttingDown.get()) {
            // DB is being shutdown; no more background compactions
        }
        else if (immutableMemTable == null &&
                manualCompaction == null &&
                !versions.needsCompaction()) {
            // No work to be done
        }
        else {
            backgroundCompaction = compactionExecutor.submit(new Callable<Void>()
            {
                @Override
                public Void call()
                        throws Exception
                {
                    try {
                        backgroundCall();
                    }
                    catch (Throwable e) {
                        // todo add logging system
                        e.printStackTrace();
                    }
                    return null;
                }
            });
        }
    }

    private void backgroundCall()
            throws IOException
    {
        mutex.lock();
        try {
            if (backgroundCompaction == null) {
                return;
            }

            try {
                if (!shuttingDown.get()) {
                    backgroundCompaction();
                }
            }
            finally {
                backgroundCompaction = null;
            }

            // Previous compaction may have produced too many files in a level,
            // so reschedule another compaction if needed.
            maybeScheduleCompaction();
        }
        finally {
            try {
                backgroundCondition.signalAll();
            }
            finally {
                mutex.unlock();
            }
        }
    }

    private void backgroundCompaction()
            throws IOException
    {
        Preconditions.checkState(mutex.isHeldByCurrentThread());

        if (immutableMemTable != null) {
            compactMemTable();
        }

        Compaction compaction;
        if (manualCompaction != null) {
            compaction = versions.compactRange(manualCompaction.level,
                    new InternalKey(manualCompaction.begin, MAX_SEQUENCE_NUMBER, ValueType.VALUE),
                    new InternalKey(manualCompaction.end, 0, ValueType.DELETION));
        } else {
            compaction = versions.pickCompaction();
        }

        if (compaction == null) {
            // no compaction
        } else if (/* !isManual && */ compaction.isTrivialMove()) {
            // Move file to next level
            Preconditions.checkState(compaction.getLevelInputs().size() == 1);
            FileMetaData fileMetaData = compaction.getLevelInputs().get(0);
            compaction.getEdit().deleteFile(compaction.getLevel(), fileMetaData.getNumber());
            compaction.getEdit().addFile(compaction.getLevel() + 1, fileMetaData);
            versions.logAndApply(compaction.getEdit());
            // log
        } else {
            CompactionState compactionState = new CompactionState(compaction);
            doCompactionWork(compactionState);
            cleanupCompaction(compactionState);
        }

        // manual compaction complete
        if (manualCompaction != null) {
            manualCompaction = null;
        }
    }

    private void cleanupCompaction(CompactionState compactionState)
    {
        Preconditions.checkState(mutex.isHeldByCurrentThread());

        if (compactionState.builder != null) {
            compactionState.builder.abandon();
        } else {
            Preconditions.checkArgument(compactionState.outfile == null);
        }

        for (FileMetaData output : compactionState.outputs) {
            pendingOutputs.remove(output.getNumber());
        }
    }

    private long recoverLogFile(long fileNumber, VersionEdit edit)
    {
        // todo implement tx log and recovery
        throw new UnsupportedOperationException();
    }

    public ChannelBuffer get(ChannelBuffer key)
    {
        return get(new ReadOptions(), key);
    }

    public ChannelBuffer get(ReadOptions options, ChannelBuffer key)
    {
        LookupKey lookupKey;
        mutex.lock();
        try {
            long snapshot = getSnapshotNumber(options);
            lookupKey = new LookupKey(key, snapshot);

            // First look in the memtable, then in the immutable memtable (if any).
            LookupResult lookupResult = memTable.get(lookupKey);
            if (lookupResult != null) {
                return lookupResult.getValue();
            }
            if (immutableMemTable != null) {
                lookupResult = immutableMemTable.get(lookupKey);
                if (lookupResult != null) {
                    return lookupResult.getValue();
                }
            }
        }
        finally {
            mutex.unlock();
        }

        // Not in memTables; try live files in level order
        LookupResult lookupResult = versions.get(lookupKey);
        if (lookupResult != null) {
            return lookupResult.getValue();
        }

        // todo schedule compaction

        return null;
    }

    public void put(ChannelBuffer key, ChannelBuffer value)
    {
        put(new WriteOptions(), key, value);
    }

    public void put(WriteOptions options, ChannelBuffer key, ChannelBuffer value)
    {
        write(options, new WriteBatch().put(key, value));
    }

    public void delete(ChannelBuffer key)
    {
        write(new WriteOptions(), new WriteBatch().delete(key));
    }

    public void delete(WriteOptions options, ChannelBuffer key)
    {
        write(options, new WriteBatch().delete(key));
    }

    public Snapshot write(WriteOptions options, WriteBatch updates)
    {
        mutex.lock();
        try {
            makeRoomForWrite(false);

            // Get sequence numbers for this change set
            final long sequenceBegin = versions.getLastSequence() + 1;
            final long sequenceEnd = sequenceBegin + updates.size() - 1;

            // Reserve this sequence in the version set
            versions.setLastSequence(sequenceEnd);

            // Log write
            final ChannelBuffer record = ChannelBuffers.dynamicBuffer();
            record.writeLong(sequenceBegin);
            record.writeInt(updates.size());
            updates.forEach(new Handler()
            {
                @Override
                public void put(ChannelBuffer key, ChannelBuffer value)
                {
                    record.writeByte(VALUE.getPersistentId());
                    writeLengthPrefixedBytes(record, key.slice());
                    writeLengthPrefixedBytes(record, value.slice());
                }

                @Override
                public void delete(ChannelBuffer key)
                {
                    record.writeByte(DELETION.getPersistentId());
                    writeLengthPrefixedBytes(record, key.slice());
                }
            });
            try {
                log.addRecord(record);
                if (options.isSync()) {
                    logChannel.force(false);
                }
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }

            // Update memtable
            final MemTable memTable = this.memTable;
            updates.forEach(new Handler()
            {
                private long sequence = sequenceBegin;

                @Override
                public void put(ChannelBuffer key, ChannelBuffer value)
                {
                    memTable.add(sequence++, VALUE, key, value);
                }

                @Override
                public void delete(ChannelBuffer key)
                {
                    memTable.add(sequence++, DELETION, key, ChannelBuffers.EMPTY_BUFFER);
                }
            });

            return new SnapshotImpl(sequenceEnd);
        }
        finally {
            mutex.unlock();
        }
    }

    @Override
    public SeekingIterator<ChannelBuffer, ChannelBuffer> iterator()
    {
        return iterator(new ReadOptions());
    }

    public SeekingIterator<ChannelBuffer, ChannelBuffer> iterator(ReadOptions options)
    {
        mutex.lock();
        try {
            SeekingIterator<InternalKey, ChannelBuffer> rawIterator = internalIterator();


            // filter any entries not visible in our snapshot
            long snapshot = getSnapshotNumber(options);
            SeekingIterator<InternalKey, ChannelBuffer> snapshotIterator = new SnapshotSeekingIterator(rawIterator, snapshot, internalKeyComparator.getUserComparator());

            // transform the keys user space
            SeekingIterator<ChannelBuffer, ChannelBuffer> userIterator = SeekingIterators.transformKeys(snapshotIterator,
                    INTERNAL_KEY_TO_USER_KEY,
                    createUserKeyToInternalKeyFunction(snapshot));
            return userIterator;
        }
        finally {
            mutex.unlock();
        }
    }

    SeekingIterable<InternalKey, ChannelBuffer> internalIterable()
    {
        return new SeekingIterable<InternalKey, ChannelBuffer>()
        {
            @Override
            public SeekingIterator<InternalKey, ChannelBuffer> iterator()
            {
                return internalIterator();
            }
        };
    }

    SeekingIterator<InternalKey, ChannelBuffer> internalIterator()
    {
        mutex.lock();
        try {
            // merge together the memTable, immutableMemTable, and tables in version set
            ImmutableList.Builder<SeekingIterator<InternalKey, ChannelBuffer>> iterators = ImmutableList.builder();
            if (memTable != null && !memTable.isEmpty()) {
                iterators.add(memTable.iterator());
            }
            if (immutableMemTable != null && !immutableMemTable.isEmpty()) {
                iterators.add(immutableMemTable.iterator());
            }
            // todo only add if versions is not empty... makes debugging the iterators easier
            iterators.add(versions.iterator());
            return SeekingIterators.merge(iterators.build(), internalKeyComparator);
        }
        finally {
            mutex.unlock();
        }
    }

    public Snapshot getSnapshot()
    {
        mutex.lock();
        try {
            return new SnapshotImpl(versions.getLastSequence());
        }
        finally {
            mutex.unlock();
        }
    }

    private long getSnapshotNumber(ReadOptions options)
    {
        long snapshot;
        if (options.getSnapshot() != null) {
            snapshot = ((SnapshotImpl) options.getSnapshot()).snapshot;
        }
        else {
            snapshot = versions.getLastSequence();
        }
        return snapshot;
    }

    private void makeRoomForWrite(boolean force)
    {
        Preconditions.checkState(mutex.isHeldByCurrentThread());

        boolean allowDelay = !force;

        while (true) {
            // todo background processing system need work
//            if (!bg_error_.ok()) {
//              // Yield previous error
//              s = bg_error_;
//              break;
//            } else
            if (allowDelay && versions.numberOfFilesInLevel(0) > L0_SLOWDOWN_WRITES_TRIGGER) {
                // We are getting close to hitting a hard limit on the number of
                // L0 files.  Rather than delaying a single write by several
                // seconds when we hit the hard limit, start delaying each
                // individual write by 1ms to reduce latency variance.  Also,
                // this delay hands over some CPU to the compaction thread in
                // case it is sharing the same core as the writer.
                try {
                    mutex.unlock();
                    Thread.sleep(1);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } finally {
                    mutex.lock();
                }

                // Do not delay a single write more than once
                allowDelay = false;
            }
            else if (!force && memTable.approximateMemoryUsage() <= options.getWriteBufferSize()) {
                // There is room in current memtable
                break;
            }
            else if (immutableMemTable != null) {
                // We have filled up the current memtable, but the previous
                // one is still being compacted, so we wait.
                backgroundCondition.awaitUninterruptibly();
            }
            else if (versions.numberOfFilesInLevel(0) >= L0_STOP_WRITES_TRIGGER) {
                // There are too many level-0 files.
//                Log(options_.info_log, "waiting...\n");
                backgroundCondition.awaitUninterruptibly();
            }
            else {
                // Attempt to switch to a new memtable and trigger compaction of old
                Preconditions.checkState(versions.getPrevLogNumber() == 0);

                // open a new log
                long logNumber = versions.getNextFileNumber();
                File file = new File(databaseDir, Filename.logFileName(logNumber));
                FileChannel channel;
                try {
                    channel = new FileOutputStream(file).getChannel();
                }
                catch (FileNotFoundException e) {
                    throw new RuntimeException("Unable to open new log file " + file.getAbsoluteFile(), e);
                }

                this.log = new LogWriter(channel);
                this.logChannel = channel;
                this.logFileNumber = logNumber;

                // create a new mem table
                immutableMemTable = memTable;
                memTable = new MemTable(internalKeyComparator);

                // Do not force another compaction there is space available
                force = false;

                maybeScheduleCompaction();
            }
        }
    }

    private void compactMemTable()
            throws IOException
    {
        Preconditions.checkState(mutex.isHeldByCurrentThread());
        Preconditions.checkState(immutableMemTable != null);

        // Save the contents of the memtable as a new Table
        VersionEdit edit = new VersionEdit();
        Version base = versions.getCurrent();
        writeLevel0Table(immutableMemTable, edit, base);

//        if (shuttingDown.get()) {
//            throw new IOException("Database shutdown during memtable compaction");
//        }

        // Replace immutable memtable with the generated Table
        edit.setPreviousLogNumber(0);
        edit.setLogNumber(logFileNumber);  // Earlier logs no longer needed
        versions.logAndApply(edit);

        immutableMemTable = null;

        deleteObsoleteFiles();
    }

    private void writeLevel0Table(MemTable memTable, VersionEdit edit, Version base)
            throws IOException
    {
        Preconditions.checkState(mutex.isHeldByCurrentThread());

        if (memTable.isEmpty()) {
            return;
        }

        // write the memtable to a new sstable
        FileMetaData fileMetaData;
        mutex.unlock();
        try {
            fileMetaData = buildTable(memTable);
        }
        finally {
            mutex.lock();
        }

        ChannelBuffer minUserKey = fileMetaData.getSmallest().getUserKey();
        ChannelBuffer maxUserKey = fileMetaData.getLargest().getUserKey();

        int level = 0;
        if (base != null && !base.overlapInLevel(0, minUserKey, maxUserKey)) {
            // Push the new sstable to a higher level if possible to reduce
            // expensive manifest file ops.
            while (level < MAX_MEM_COMPACT_LEVEL && !base.overlapInLevel(level + 1, minUserKey, maxUserKey)) {
                level++;
            }
        }
        edit.addFile(level, fileMetaData);
    }

    private FileMetaData buildTable(SeekingIterable<InternalKey, ChannelBuffer> data)
            throws IOException
    {
        long fileNumber = versions.getNextFileNumber();
        pendingOutputs.add(fileNumber);
        File file = new File(databaseDir, Filename.tableFileName(fileNumber));

        try {
            FileChannel channel = new FileOutputStream(file).getChannel();
            TableBuilder tableBuilder = new TableBuilder(options, channel, new InternalUserComparator(internalKeyComparator));

            InternalKey smallest = null;
            InternalKey largest = null;
            for (Entry<InternalKey, ChannelBuffer> entry : data) {
                // update keys
                InternalKey key = entry.getKey();
                if (smallest == null) {
                    smallest = key;
                }
                largest = key;

                tableBuilder.add(key.encode(), entry.getValue());
            }

            tableBuilder.finish();

            channel.force(true);
            channel.close();

            if (smallest == null) {
                return null;
            }
            FileMetaData fileMetaData = new FileMetaData(fileNumber, file.length(), smallest, largest);

            // verify table can be opened
            tableCache.newIterator(fileMetaData);

            pendingOutputs.remove(fileNumber);

            return fileMetaData;
        }
        catch (IOException e) {
            file.delete();
            throw e;
        }
    }

    private void doCompactionWork(CompactionState compactionState)
            throws IOException
    {
        Preconditions.checkState(mutex.isHeldByCurrentThread());
        Preconditions.checkArgument(versions.numberOfBytesInLevel(compactionState.getCompaction().getLevel()) > 0);
        Preconditions.checkArgument(compactionState.builder == null);
        Preconditions.checkArgument(compactionState.outfile == null);

        // todo track snapshots
        compactionState.smallestSnapshot = versions.getLastSequence();

        // Release mutex while we're actually doing the compaction work
        mutex.unlock();
        try {
            SeekingIterator<InternalKey, ChannelBuffer> iterator = versions.makeInputIterator(compactionState.compaction);

            ChannelBuffer currentUserKey = null;
            boolean hasCurrentUserKey = false;

            long lastSequenceForKey = MAX_SEQUENCE_NUMBER;
            while (iterator.hasNext() && !shuttingDown.get()) {
                // always give priority to compacting the current mem table
                if (immutableMemTable != null) {
                    mutex.lock();
                    try {
                        compactMemTable();
                    }
                    finally {
                        try {
                            backgroundCondition.signalAll();
                        }
                        finally {
                            mutex.unlock();
                        }
                    }
                }

                InternalKey key = iterator.peek().getKey();
                if (compactionState.compaction.shouldStopBefore(key) && compactionState.builder != null) {
                    finishCompactionOutputFile(compactionState, iterator);
                }

                // Handle key/value, add to state, etc.
                boolean drop = false;
                // todo if key doesn't parse (it is corrupted),
                if (false /*!ParseInternalKey(key, &ikey)*/) {
                    // do not hide error keys
                    currentUserKey = null;
                    hasCurrentUserKey = false;
                    lastSequenceForKey = MAX_SEQUENCE_NUMBER;
                }
                else {
                    if (!hasCurrentUserKey || internalKeyComparator.getUserComparator().compare(key.getUserKey(), currentUserKey) != 0) {
                        // First occurrence of this user key
                        currentUserKey = key.getUserKey();
                        hasCurrentUserKey = true;
                        lastSequenceForKey = MAX_SEQUENCE_NUMBER;
                    }

                    if (lastSequenceForKey <= compactionState.smallestSnapshot) {
                        // Hidden by an newer entry for same user key
                        drop = true; // (A)
                    }
                    else if (key.getValueType() == ValueType.DELETION &&
                            key.getSequenceNumber() <= compactionState.smallestSnapshot &&
                            compactionState.compaction.isBaseLevelForKey(key.getUserKey())) {

                        // For this user key:
                        // (1) there is no data in higher levels
                        // (2) data in lower levels will have larger sequence numbers
                        // (3) data in layers that are being compacted here and have
                        //     smaller sequence numbers will be dropped in the next
                        //     few iterations of this loop (by rule (A) above).
                        // Therefore this deletion marker is obsolete and can be dropped.
                        drop = true;
                    }

                    lastSequenceForKey = key.getSequenceNumber();
                }

                if (!drop) {
                    // Open output file if necessary
                    if (compactionState.builder == null) {
                        openCompactionOutputFile(compactionState);
                    }
                    if (compactionState.builder.getEntryCount() == 0) {
                        compactionState.currentSmallest = key;
                    }
                    compactionState.currentLargest = key;
                    compactionState.builder.add(key.encode(), iterator.peek().getValue());

                    // Close output file if it is big enough
                    if (compactionState.builder.getFileSize() >=
                            compactionState.compaction.MaxOutputFileSize()) {
                        finishCompactionOutputFile(compactionState, iterator);
                    }
                }
                iterator.next();
            }

//            if (shuttingDown.get()) {
//                throw new IOException("DB shutdown during compaction");
//            }
            if (compactionState.builder != null) {
                finishCompactionOutputFile(compactionState, iterator);
            }
        }
        finally {
            mutex.lock();
        }

        // todo port CompactionStats code

        installCompactionResults(compactionState);
    }

    private void openCompactionOutputFile(CompactionState compactionState)
            throws FileNotFoundException
    {
        Preconditions.checkNotNull(compactionState, "compactionState is null");
        Preconditions.checkArgument(compactionState.builder == null, "compactionState builder is not null");

        mutex.lock();
        try {
            long fileNumber = versions.getNextFileNumber();
            pendingOutputs.add(fileNumber);
            compactionState.currentFileNumber = fileNumber;
            compactionState.currentFileSize = 0;
            compactionState.currentSmallest = null;
            compactionState.currentLargest = null;

            File file = new File(databaseDir, Filename.tableFileName(fileNumber));
            compactionState.outfile = new FileOutputStream(file).getChannel();
            compactionState.builder = new TableBuilder(options, compactionState.outfile, new InternalUserComparator(internalKeyComparator));
        }
        finally {
            mutex.unlock();
        }
    }

    private void finishCompactionOutputFile(CompactionState compactionState, SeekingIterator<InternalKey, ChannelBuffer> input)
            throws IOException
    {
        Preconditions.checkNotNull(compactionState, "compactionState is null");
        Preconditions.checkArgument(compactionState.outfile != null);
        Preconditions.checkArgument(compactionState.builder != null);

        long outputNumber = compactionState.currentFileNumber;
        Preconditions.checkArgument(outputNumber != 0);

        long currentEntries = compactionState.builder.getEntryCount();
        compactionState.builder.finish();

        long currentBytes = compactionState.builder.getFileSize();
        compactionState.currentFileSize = currentBytes;
        compactionState.totalBytes += currentBytes;

        FileMetaData currentFileMetaData = new FileMetaData(compactionState.currentFileNumber,
                compactionState.currentFileSize,
                compactionState.currentSmallest,
                compactionState.currentLargest);
        compactionState.outputs.add(currentFileMetaData);

        compactionState.builder = null;

        compactionState.outfile.force(true);
        compactionState.outfile.close();
        compactionState.outfile = null;

        if (currentEntries > 0) {
            // Verify that the table is usable
            tableCache.newIterator(outputNumber);
        }
    }

    private void installCompactionResults(CompactionState compact)
            throws IOException
    {
        Preconditions.checkState(mutex.isHeldByCurrentThread());

        // Add compaction outputs
        compact.compaction.addInputDeletions(compact.compaction.getEdit());
        int level = compact.compaction.getLevel();
        for (FileMetaData output : compact.outputs) {
            compact.compaction.getEdit().addFile(level + 1, output);
            pendingOutputs.remove(output.getNumber());
        }
        compact.outputs.clear();

        try {
            versions.logAndApply(compact.compaction.getEdit());
            deleteObsoleteFiles();
        }
        catch (IOException e) {
            // Discard any files we may have created during this failed compaction
            for (FileMetaData output : compact.outputs) {
                new File(databaseDir, Filename.tableFileName(output.getNumber())).delete();
            }
        }
    }

    int numberOfFilesInLevel(int level)
    {
        return versions.getCurrent().numberOfFilesInLevel(level);
    }

    private static class CompactionState
    {
        private final Compaction compaction;

        private final List<FileMetaData> outputs = newArrayList();

        private long smallestSnapshot;

        // State kept for output being generated
        private FileChannel outfile;
        private TableBuilder builder;

        // Current file being generated
        private long currentFileNumber;
        private long currentFileSize;
        private InternalKey currentSmallest;
        private InternalKey currentLargest;

        private long totalBytes;

        private CompactionState(Compaction compaction)
        {
            this.compaction = compaction;
        }

        public Compaction getCompaction()
        {
            return compaction;
        }
    }

    private static class ManualCompaction
    {
        private final int level;
        private final ChannelBuffer begin;
        private final ChannelBuffer end;

        private ManualCompaction(int level, ChannelBuffer begin, ChannelBuffer end)
        {
            this.level = level;
            this.begin = begin;
            this.end = end;
        }
    }
}
