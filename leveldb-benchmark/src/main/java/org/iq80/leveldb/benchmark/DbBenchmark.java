/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.leveldb.benchmark;

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.impl.DbImpl;
import org.iq80.leveldb.table.BloomFilterPolicy;
import org.iq80.leveldb.util.Closeables;
import org.iq80.leveldb.util.FileUtils;
import org.iq80.leveldb.util.PureJavaCrc32C;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.SliceOutput;
import org.iq80.leveldb.util.Slices;
import org.iq80.leveldb.util.Snappy;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.iq80.leveldb.impl.DbConstants.NUM_LEVELS;

public class DbBenchmark
{
    private final boolean useExisting;
    private final Integer writeBufferSize;
    private final File databaseDir;
    private final double compressionRatio;
    private final Map<Flag, Object> flags;

    //    Cache cache_;
    private final List<String> benchmarks;
    private final int blockCacheSize;
    private final int bloomFilterBits;
    private DB db;
    private int num;
    private int reads;
    private int valueSize;
    private WriteOptions writeOptions;
    private int entriesPerBatch;

    private final DBFactory factory;

    public DbBenchmark(Map<Flag, Object> flags)
            throws Exception
    {
        ClassLoader cl = DbBenchmark.class.getClassLoader();
        factory = (DBFactory) cl.loadClass(System.getProperty("leveldb.factory", "org.iq80.leveldb.impl.Iq80DBFactory")).newInstance();
        this.flags = flags;
        benchmarks = (List<String>) flags.get(Flag.benchmarks);

        writeBufferSize = (Integer) flags.get(Flag.write_buffer_size);
        compressionRatio = (Double) flags.get(Flag.compression_ratio);
        useExisting = (Boolean) flags.get(Flag.use_existing_db);
        blockCacheSize = (Integer) flags.get(Flag.cache_size);
        bloomFilterBits = (Integer) flags.get(Flag.bloom_bits);
        num = (Integer) flags.get(Flag.num);
        reads = (Integer) (flags.get(Flag.reads) == null ? flags.get(Flag.num) : flags.get(Flag.reads));
        valueSize = (Integer) flags.get(Flag.value_size);
        entriesPerBatch = 1;

        databaseDir = new File((String) flags.get(Flag.db));

        // delete heap files in db
        for (File file : FileUtils.listFiles(databaseDir)) {
            if (file.getName().startsWith("heap-")) {
                file.delete();
            }
        }

        if (!useExisting) {
            destroyDb();
        }
    }

    private void run()
            throws IOException
    {
        printHeader();
        open();

        for (String benchmark : benchmarks) {
            // Reset parameters that may be overridden below
            num = (Integer) flags.get(Flag.num);
            reads = (Integer) (flags.get(Flag.reads) == null ? flags.get(Flag.num) : flags.get(Flag.reads));
            valueSize = (Integer) flags.get(Flag.value_size);
            entriesPerBatch = 1;
            writeOptions = new WriteOptions();

            boolean freshBb = false;
            int numThreads = (Integer) flags.get(Flag.threads);

            String method = null;

            if (benchmark.equals("fillseq")) {
                freshBb = true;
                method = "writeSeq";
            }
            else if (benchmark.equals("fillbatch")) {
                freshBb = true;
                entriesPerBatch = 1000;
                method = "writeSeq";
            }
            else if (benchmark.equals("fillrandom")) {
                freshBb = true;
                method = "writeRandom";
            }
            else if (benchmark.equals("overwrite")) {
                freshBb = false;
                method = "writeRandom";
            }
            else if (benchmark.equals("fillsync")) {
                freshBb = true;
                num /= 1000;
                writeOptions.sync(true);
                method = "writeRandom";
            }
            else if (benchmark.equals("fill100K")) {
                freshBb = true;
                num /= 1000;
                valueSize = 100 * 1000;
                method = "writeRandom";
            }
            else if (benchmark.equals("readseq")) {
                method = "readSequential";
            }
            else if (benchmark.equals("readreverse")) {
                method = "readReverse";
            }
            else if (benchmark.equals("readrandom")) {
                method = "readRandom";
            }
            else if (benchmark.equals("seekrandom")) {
                method = "seekRandom";
            }
            else if (benchmark.equals("readhot")) {
                method = "readHot";
            }
            else if (benchmark.equals("readrandomsmall")) {
                reads /= 1000;
                method = "readRandom";
            }
            else if (benchmark.equals("readwhilewriting")) {
                numThreads++;  // Add extra thread for writing
                method = "readWhileWriting";
            }
            else if (benchmark.equals("compact")) {
                method = "compact";
            }
            else if (benchmark.equals("crc32c")) {
                method = "crc32c";
            }
            else if (benchmark.equals("acquireload")) {
                method = "acquireLoad";
            }
            else if (benchmark.equals("snappycomp")) {
                if (Snappy.available()) {
                    method = "snappyCompress";
                }
            }
            else if (benchmark.equals("snappyuncomp")) {
                if (Snappy.available()) {
                    method = "snappyUncompressDirectBuffer";
                }
            }
            else if (benchmark.equals("unsnap-array")) {
                if (Snappy.available()) {
                    method = "snappyUncompressArray";
                }
            }
            else if (benchmark.equals("unsnap-direct")) {
                if (Snappy.available()) {
                    method = "snappyUncompressDirectBuffer";
                }
            }
            else if (benchmark.equals("heapprofile")) {
                heapProfile();
            }
            else if (benchmark.equals("stats")) {
                printStats();
            }
            else {
                System.err.println("Unknown benchmark: " + benchmark);
            }
            if (freshBb) {
                if (useExisting) {
                    System.out.println("skipping (--use_existing_db is true)");
                    return;
                }
                db.close();
                db = null;
                destroyDb();
                open();
            }
            if (method != null) {
                try {
                    runBenchmark(numThreads, benchmark, method);
                }
                catch (Exception e) {
                    System.out.println("Failed to rung " + method);
                    e.printStackTrace();
                    return;
                }
            }

        }
        db.close();
    }

    private void runBenchmark(int n, String name, String method) throws Exception
    {
        SharedState shared = new SharedState();

        ThreadArg[] arg = new ThreadArg[n];
        for (int i = 0; i < arg.length; ++i) {
            arg[i] = new ThreadArg();
            arg[i].bm = this;
            arg[i].method = method;
            arg[i].shared = shared;
            arg[i].thread = new ThreadState(i);
            arg[i].thread.shared = shared;
            startThread(arg[i]);
        }

        shared.mu.lock();
        while (shared.numInitialized < n) {
            shared.cv.await();
        }

        shared.start = true;
        shared.cv.signalAll();
        while (shared.numDone < n) {
            shared.cv.await();
        }
        shared.mu.unlock();

        for (int i = 1; i < n; i++) {
            arg[0].thread.stats.merge(arg[i].thread.stats);
        }
        arg[0].thread.stats.report(name);
    }

    public void startThread(final ThreadArg arg)
    {
        new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                SharedState shared = arg.shared;
                ThreadState thread = arg.thread;
                shared.mu.lock();
                try {
                    shared.numInitialized++;
                    if (shared.numInitialized >= shared.total) {
                        shared.cv.signalAll();
                    }
                    while (!shared.start) {
                        shared.cv.awaitUninterruptibly();
                    }
                }
                finally {
                    shared.mu.unlock();
                }
                try {
                    Method method = arg.bm.getClass().getDeclaredMethod(arg.method, ThreadState.class);
                    method.setAccessible(true);
                    thread.stats.start();
                    method.invoke(arg.bm, thread);
                }
                catch (Exception e) {
                    thread.stats.addMessage("ERROR " + e);
                    e.printStackTrace();
                }
                finally {
                    thread.stats.stop();
                }

                shared.mu.lock();
                try {
                    shared.numDone++;
                    if (shared.numDone >= shared.total) {
                        shared.cv.signalAll();
                    }
                }
                finally {
                    shared.mu.unlock();
                }
            }
        }).start();
    }

    private void printHeader()
            throws IOException
    {
        int kKeySize = 16;
        printEnvironment();
        System.out.printf("Keys:       %d bytes each\n", kKeySize);
        System.out.printf("Values:     %d bytes each (%d bytes after compression)\n",
                valueSize,
                (int) (valueSize * compressionRatio + 0.5));
        System.out.printf("Entries:    %d\n", num);
        System.out.printf("RawSize:    %.1f MB (estimated)\n",
                ((kKeySize + valueSize) * num) / 1048576.0);
        System.out.printf("FileSize:   %.1f MB (estimated)\n",
                (((kKeySize + valueSize * compressionRatio) * num)
                        / 1048576.0));
        printWarnings();
        System.out.printf("------------------------------------------------\n");
    }

    @SuppressWarnings({"InnerAssignment"})
    static void printWarnings()
    {
        boolean assertsEnabled = false;
        // CHECKSTYLE IGNORE check FOR NEXT 1 LINES
        assert assertsEnabled = true;  // Intentional side effect!!!
        if (assertsEnabled) {
            System.out.printf("WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
        }

        // See if snappy is working by attempting to compress a compressible string
        String text = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
        byte[] compressedText = null;
        try {
            compressedText = Snappy.compress(text);
        }
        catch (Exception ignored) {
        }
        if (compressedText == null) {
            System.out.printf("WARNING: Snappy compression is not enabled\n");
        }
        else if (compressedText.length > text.length()) {
            System.out.printf("WARNING: Snappy compression is not effective\n");
        }
    }

    void printEnvironment()
            throws IOException
    {
        System.out.printf("LevelDB:    %s\n", factory);

        System.out.printf("Date:       %tc\n", new Date());

        File cpuInfo = new File("/proc/cpuinfo");
        if (cpuInfo.canRead()) {
            int numberOfCpus = 0;
            String cpuType = null;
            String cacheSize = null;
            for (String line : CharStreams.readLines(Files.newReader(cpuInfo, UTF_8))) {
                ImmutableList<String> parts = ImmutableList.copyOf(Splitter.on(':').omitEmptyStrings().trimResults().limit(2).split(line));
                if (parts.size() != 2) {
                    continue;
                }
                String key = parts.get(0);
                String value = parts.get(1);

                if (key.equals("model name")) {
                    numberOfCpus++;
                    cpuType = value;
                }
                else if (key.equals("cache size")) {
                    cacheSize = value;
                }
            }
            System.out.printf("CPU:        %d * %s\n", numberOfCpus, cpuType);
            System.out.printf("CPUCache:   %s\n", cacheSize);
        }
    }

    private void open()
            throws IOException
    {
        Options options = new Options();
        options.createIfMissing(!useExisting);
        if (blockCacheSize >= 0) {
            options.cacheSize(blockCacheSize);
        }
        if (bloomFilterBits >= 0) {
            options.filterPolicy(new BloomFilterPolicy(bloomFilterBits));
        }
        options.cacheSize(blockCacheSize);
        if (writeBufferSize != null) {
            options.writeBufferSize(writeBufferSize);
        }
        db = factory.open(databaseDir, options);
    }

    private void write(ThreadState thread, boolean seq)
            throws IOException
    {
        if (!flags.get(Flag.num).equals(num)) {
            thread.stats.addMessage(String.format("(%d ops)", num));
        }

        RandomGenerator gen = newGenerator();
        long bytes = 0;
        for (int i = 0; i < num; i += entriesPerBatch) {
            WriteBatch batch = db.createWriteBatch();
            for (int j = 0; j < entriesPerBatch; j++) {
                int k = seq ? i + j : thread.rand.nextInt(num);
                byte[] key = formatNumber(k);
                batch.put(key, gen.generate(valueSize));
                bytes += valueSize + key.length;
                thread.stats.finishedSingleOp();
            }
            db.write(batch, writeOptions);
            batch.close();
        }
        thread.stats.addBytes(bytes);
    }

    public static byte[] formatNumber(long n)
    {
        checkArgument(n >= 0, "number must be positive");

        byte[] slice = new byte[16];

        int i = 15;
        while (n > 0) {
            slice[i--] = (byte) ((long) '0' + (n % 10));
            n /= 10;
        }
        while (i >= 0) {
            slice[i--] = '0';
        }
        return slice;
    }

    private void readSequential(ThreadState thread)
    {
        long bytes = 0;
        for (int loops = 0; loops < 5; loops++) {
            DBIterator iterator = db.iterator();
            for (int i = 0; i < reads && iterator.hasNext(); i++) {
                Map.Entry<byte[], byte[]> entry = iterator.next();
                bytes += entry.getKey().length + entry.getValue().length;
                thread.stats.finishedSingleOp();
            }
            Closeables.closeQuietly(iterator);
        }
        thread.stats.addBytes(bytes);
    }

    private void readReverse(ThreadState thread)
    {
        //To change body of created methods use File | Settings | File Templates.
    }

    private void readRandom(ThreadState thread)
    {
        int found = 0;
        for (int i = 0; i < reads; i++) {
            byte[] key = formatNumber(thread.rand.nextInt(num));
            byte[] value = db.get(key);
            if (value != null) {
                found++;
            }
            thread.stats.finishedSingleOp();
        }
        thread.stats.addMessage(String.format("(%d of %d found)", found, num));
    }

    private void readMissing(ThreadState thread)
    {

        for (int i = 0; i < reads; i++) {
            byte[] key = formatNumber(thread.rand.nextInt(num));
            db.get(key);
            thread.stats.finishedSingleOp();
        }
    }

    private void readHot(ThreadState thread)
    {
        long bytes = 0;
        int range = (num + 99) / 100;
        for (int i = 0; i < reads; i++) {
            byte[] key = formatNumber(thread.rand.nextInt(range));
            byte[] value = db.get(key);
            bytes += key.length + value.length;
            thread.stats.finishedSingleOp();
        }
        thread.stats.addBytes(bytes);
    }

    private void seekRandom(ThreadState thread) throws IOException
    {
        ReadOptions options = new ReadOptions();
        int found = 0;
        for (int i = 0; i < reads; i++) {
            DBIterator iter = db.iterator(options);
            byte[] key = formatNumber(thread.rand.nextInt(num));
            iter.seek(key);
            if (iter.hasNext() == Arrays.equals(iter.next().getKey(), key)) {
                found++;
            }
            iter.close();
            thread.stats.finishedSingleOp();
        }
        thread.stats.addMessage(String.format("(%d of %d found)", found, num));
    }

    private void readWhileWriting(ThreadState thread)
    {
        if (thread.tid > 0) {
            readRandom(thread);
        }
        else {
            // Special thread that keeps writing until other threads are done.
            RandomGenerator gen = newGenerator();
            while (true) {
                thread.shared.mu.lock();
                try {
                    if (thread.shared.numDone + 1 >= thread.shared.numInitialized) {
                        // Other threads have finished
                        break;
                    }
                }
                finally {
                    thread.shared.mu.unlock();
                }

                byte[] key = formatNumber(thread.rand.nextInt((Integer) flags.get(Flag.num)));
                db.put(key, gen.generate(valueSize), writeOptions);
            }

            // Do not count any of the preceding work/delay in stats.
            thread.stats.start();
        }
    }

    private void compact(ThreadState thread)
            throws IOException
    {
        if (db instanceof DbImpl) {
            ((DbImpl) db).compactMemTable();
            for (int level = 0; level < NUM_LEVELS - 1; level++) {
                ((DbImpl) db).compactRange(level, Slices.copiedBuffer("", UTF_8), Slices.copiedBuffer("~", UTF_8));
            }
        }
    }

    private void crc32c(final ThreadState thread)
    {
        // Checksum about 500MB of data total
        int blockSize = 4096;
        String label = "(4K per op)";
        // Checksum about 500MB of data total
        byte[] data = new byte[blockSize];
        Arrays.fill(data, (byte) 'x');

        long bytes = 0;
        int crc = 0;
        while (bytes < 1000 * 1048576) {
            PureJavaCrc32C checksum = new PureJavaCrc32C();
            checksum.update(data, 0, blockSize);
            crc = checksum.getMaskedValue();
            thread.stats.finishedSingleOp();
            bytes += blockSize;
        }
        // Print so result is not dead
        System.out.printf("... crc=0x%x\r", crc);

        thread.stats.addBytes(bytes);
        thread.stats.addMessage(label);
    }

    private void acquireLoad(ThreadState thread)
    {
        //To change body of created methods use File | Settings | File Templates.
    }

    private void snappyCompress(ThreadState thread)
    {
        byte[] raw = newGenerator().generate(new Options().blockSize());
        byte[] compressedOutput = new byte[Snappy.maxCompressedLength(raw.length)];

        long bytes = 0;
        long produced = 0;

        // attempt to compress the block
        while (bytes < 1024 * 1048576) {  // Compress 1G
            try {
                int compressedSize = Snappy.compress(raw, 0, raw.length, compressedOutput, 0);
                bytes += raw.length;
                produced += compressedSize;
            }
            catch (IOException ignored) {
                thread.stats.addMessage("(snappy failure)");
                throw Throwables.propagate(ignored);
            }

            thread.stats.finishedSingleOp();
        }
        thread.stats.addMessage(String.format("(output: %.1f%%)", (produced * 100.0) / bytes));
        thread.stats.addBytes(bytes);
    }

    private RandomGenerator newGenerator()
    {
        return new RandomGenerator(compressionRatio);
    }

    private void snappyUncompressArray(ThreadState thread)
    {
        int inputSize = new Options().blockSize();
        byte[] compressedOutput = new byte[Snappy.maxCompressedLength(inputSize)];
        byte[] raw = newGenerator().generate(inputSize);
        long bytes = 0;
        int compressedLength;
        try {
            compressedLength = Snappy.compress(raw, 0, raw.length, compressedOutput, 0);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        // attempt to uncompress the block
        while (bytes < 5L * 1024 * 1048576) {  // Compress 1G
            try {
                Snappy.uncompress(compressedOutput, 0, compressedLength, raw, 0);
                bytes += inputSize;
            }
            catch (IOException ignored) {
                thread.stats.addMessage("(snappy failure)");
                throw Throwables.propagate(ignored);
            }

            thread.stats.finishedSingleOp();
        }
        thread.stats.addBytes(bytes);
    }

    private void snappyUncompressDirectBuffer(ThreadState thread)
    {
        int inputSize = new Options().blockSize();
        byte[] compressedOutput = new byte[Snappy.maxCompressedLength(inputSize)];
        byte[] raw = newGenerator().generate(inputSize);
        int compressedLength;
        try {
            compressedLength = Snappy.compress(raw, 0, raw.length, compressedOutput, 0);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        ByteBuffer uncompressedBuffer = ByteBuffer.allocateDirect(inputSize);
        ByteBuffer compressedBuffer = ByteBuffer.allocateDirect(compressedLength);
        compressedBuffer.put(compressedOutput, 0, compressedLength);

        long bytes = 0;
        // attempt to uncompress the block
        while (bytes < 5L * 1024 * 1048576) {  // Compress 1G
            try {
                uncompressedBuffer.clear();
                compressedBuffer.position(0);
                compressedBuffer.limit(compressedLength);
                Snappy.uncompress(compressedBuffer, uncompressedBuffer);
                bytes += inputSize;
            }
            catch (IOException ignored) {
                thread.stats.addMessage("(snappy failure)");
                throw Throwables.propagate(ignored);
            }

            thread.stats.finishedSingleOp();
            thread.stats.addBytes(bytes);
        }
    }

    private void writeSeq(ThreadState thread) throws IOException
    {
        write(thread, true);
    }

    private void writeRandom(ThreadState thread) throws IOException
    {
        write(thread, false);
    }

    private void heapProfile()
    {
        //To change body of created methods use File | Settings | File Templates.
    }

    private void destroyDb()
    {
        Closeables.closeQuietly(db);
        db = null;
        FileUtils.deleteRecursively(databaseDir);
    }

    private void printStats()
    {
        //To change body of created methods use File | Settings | File Templates.
    }

    public static void main(String[] args)
            throws Exception
    {
        Map<Flag, Object> flags = new EnumMap<>(Flag.class);
        for (Flag flag : Flag.values()) {
            flags.put(flag, flag.getDefaultValue());
        }
        for (String arg : args) {
            boolean valid = false;
            if (arg.startsWith("--")) {
                try {
                    ImmutableList<String> parts = ImmutableList.copyOf(Splitter.on("=").limit(2).split(arg.substring(2)));
                    Flag key = Flag.valueOf(parts.get(0));
                    Object value = key.parseValue(parts.get(1));
                    flags.put(key, value);
                    valid = true;
                }
                catch (Exception e) {
                }
            }

            if (!valid) {
                System.err.println("Invalid argument " + arg);
                System.exit(1);
            }

        }
        new DbBenchmark(flags).run();
    }

    private enum Flag
    {
        // Comma-separated list of operations to run in the specified order
        //   Actual benchmarks:
        //      fillseq       -- write N values in sequential key order in async mode
        //      fillrandom    -- write N values in random key order in async mode
        //      overwrite     -- overwrite N values in random key order in async mode
        //      fillsync      -- write N/100 values in random key order in sync mode
        //      fill100K      -- write N/1000 100K values in random order in async mode
        //      readseq       -- read N times sequentially
        //      readreverse   -- read N times in reverse order
        //      readrandom    -- read N times in random order
        //      readhot       -- read N times in random order from 1% section of DB
        //      crc32c        -- repeated crc32c of 4K of data
        //      acquireload   -- load N*1000 times
        //   Meta operations:
        //      compact     -- Compact the entire DB
        //      stats       -- Print DB stats
        //      heapprofile -- Dump a heap profile (if supported by this port)
        benchmarks(ImmutableList.of(
                "fillseq",
                "fillseq",
                "fillseq",
                "fillsync",
                "fillrandom",
                "overwrite",
                "fillseq",
                "readrandom",
                "readrandom",  // Extra run to allow previous compactions to quiesce
                "readseq",
                // "readreverse",
                "compact",
                "readrandom",
                "readseq",
                // "readreverse",
                "fill100K",
                // "crc32c",
                "snappycomp",
                "unsnap-array",
                "unsnap-direct"
                // "acquireload"
        )) {
            @Override
            public Object parseValue(String value)
            {
                return ImmutableList.copyOf(Splitter.on(",").trimResults().omitEmptyStrings().split(value));
            }
        },

        // Arrange to generate values that shrink to this fraction of
        // their original size after compression
        compression_ratio(0.5d) {
            @Override
            public Object parseValue(String value)
            {
                return Double.parseDouble(value);
            }
        },

        // Print histogram of operation timings
        histogram(false) {
            @Override
            public Object parseValue(String value)
            {
                return Boolean.parseBoolean(value);
            }
        },

        // If true, do not destroy the existing database.  If you set this
        // flag and also specify a benchmark that wants a fresh database, that
        // benchmark will fail.
        use_existing_db(false) {
            @Override
            public Object parseValue(String value)
            {
                return Boolean.parseBoolean(value);
            }
        },

        // Number of key/values to place in database
        num(1000000) {
            @Override
            public Object parseValue(String value)
            {
                return Integer.parseInt(value);
            }
        },

        // Number of read operations to do.  If negative, do FLAGS_num reads.
        reads(null) {
            @Override
            public Object parseValue(String value)
            {
                return Integer.parseInt(value);
            }
        },

        // Number of concurrent threads to run.
        threads(1) {
            @Override
            public Object parseValue(String value)
            {
                return Integer.parseInt(value);
            }
        },

        // Size of each value
        value_size(100) {
            @Override
            public Object parseValue(String value)
            {
                return Integer.parseInt(value);
            }
        },

        // Number of bytes to buffer in memtable before compacting
        // (initialized to default value by "main")
        write_buffer_size(null) {
            @Override
            public Object parseValue(String value)
            {
                return Integer.parseInt(value);
            }
        },

        // Number of bytes to use as a cache of uncompressed data.
        // Negative means use default settings.
        cache_size(-1) {
            @Override
            public Object parseValue(String value)
            {
                return Integer.parseInt(value);
            }
        },

        // Bloom filter bits per key.
        // Negative means use default settings.
        bloom_bits(-1) {
            @Override
            public Object parseValue(String value)
            {
                return Integer.parseInt(value);
            }
        },

        // Maximum number of files to keep open at the same time (use default if == 0)
        open_files(0) {
            @Override
            public Object parseValue(String value)
            {
                return Integer.parseInt(value);
            }
        },

        // Use the db with the following name.
        db("/tmp/dbbench") {
            @Override
            public Object parseValue(String value)
            {
                return value;
            }
        };

        private final Object defaultValue;

        Flag(Object defaultValue)
        {
            this.defaultValue = defaultValue;
        }

        protected abstract Object parseValue(String value);

        public Object getDefaultValue()
        {
            return defaultValue;
        }
    }

    private static class RandomGenerator
    {
        private final Slice data;
        private int position;

        private RandomGenerator(double compressionRatio)
        {
            // We use a limited amount of data over and over again and ensure
            // that it is larger than the compression window (32KB), and also
            // large enough to serve all typical value sizes we want to write.
            Random rnd = new Random(301);
            data = Slices.allocate(1048576 + 100);
            SliceOutput sliceOutput = data.output();
            while (sliceOutput.size() < 1048576) {
                // Add a short fragment that is as compressible as specified
                // by FLAGS_compression_ratio.
                sliceOutput.writeBytes(compressibleString(rnd, compressionRatio, 100));
            }
        }

        private byte[] generate(int length)
        {
            if (position + length > data.length()) {
                position = 0;
                assert (length < data.length());
            }
            Slice slice = data.slice(position, length);
            position += length;
            return slice.getBytes();
        }
    }

    private static Slice compressibleString(Random rnd, double compressionRatio, int len)
    {
        int raw = (int) (len * compressionRatio);
        if (raw < 1) {
            raw = 1;
        }
        Slice rawData = generateRandomSlice(rnd, raw);

        // Duplicate the random data until we have filled "len" bytes
        Slice dst = Slices.allocate(len);
        SliceOutput sliceOutput = dst.output();
        while (sliceOutput.size() < len) {
            sliceOutput.writeBytes(rawData, 0, Math.min(rawData.length(), sliceOutput.writableBytes()));
        }
        return dst;
    }

    private static Slice generateRandomSlice(Random random, int length)
    {
        Slice rawData = Slices.allocate(length);
        SliceOutput sliceOutput = rawData.output();
        while (sliceOutput.isWritable()) {
            sliceOutput.writeByte((byte) ((int) ' ' + random.nextInt(95)));
        }
        return rawData;
    }

    private static class SharedState
    {
        ReentrantLock mu;
        Condition cv;
        int total;

        // Each thread goes through the following states:
        //    (1) initializing
        //    (2) waiting for others to be initialized
        //    (3) running
        //    (4) done
        int numInitialized;
        int numDone;
        boolean start;

        public SharedState()
        {
            this.mu = new ReentrantLock();
            this.cv = mu.newCondition();
        }
    }

    private class ThreadState
    {
        int tid;             // 0..n-1 when running in n threads
        Random rand;         // Has different seeds for different threads
        DbBenchmark.Stats stats = new Stats();
        SharedState shared;

        public ThreadState(int index)
        {
            this.tid = index;
            this.rand = new Random(1000 + index);
        }
    }

    private class ThreadArg
    {
        DbBenchmark bm;
        SharedState shared;
        ThreadState thread;
        String method;
    }

    private class Stats
    {
        long start;
        long finish;
        double seconds;
        int done;
        int nextReport;
        long bytes;
        double lastOpFinish;
        Histogram hist = new Histogram();
        StringBuilder message = new StringBuilder();

        public Stats()
        {
            start();
        }

        void start()
        {
            nextReport = 100;
            lastOpFinish = start;
            hist.clear();
            done = 0;
            bytes = 0;
            seconds = 0;
            start = System.nanoTime();
            finish = start;
            message.setLength(0);
        }

        void merge(Stats other)
        {
            hist.merge(other.hist);
            done += other.done;
            bytes += other.bytes;
            seconds += other.seconds;
            if (other.start < start) {
                start = other.start;
            }
            if (other.finish > finish) {
                finish = other.finish;
            }

            // Just keep the messages from one thread
            if (message.length() == 0) {
                message = other.message;
            }
        }

        void stop()
        {
            finish = System.nanoTime();
            seconds = 1.0d * (finish - start) / TimeUnit.SECONDS.toNanos(1);
        }

        void addMessage(String msg)
        {
            if (message.length() != 0) {
                message.append(" ");
            }
            message.append(msg);
        }

        void finishedSingleOp()
        {
            if (flags.containsKey(Flag.histogram)) {
                double now = System.nanoTime();
                double micros = (now - lastOpFinish) / 1000.0d;
                hist.add(micros);
                if (micros > 20000) {
                    System.out.printf("long op: %.1f micros%30s\r", micros, "");
                }
                lastOpFinish = now;
            }

            done++;
            if (done >= nextReport) {
                if (nextReport < 1000) {
                    nextReport += 100;
                }
                else if (nextReport < 5000) {
                    nextReport += 500;
                }
                else if (nextReport < 10000) {
                    nextReport += 1000;
                }
                else if (nextReport < 50000) {
                    nextReport += 5000;
                }
                else if (nextReport < 100000) {
                    nextReport += 10000;
                }
                else if (nextReport < 500000) {
                    nextReport += 50000;
                }
                else {
                    nextReport += 100000;
                }
                System.out.printf("... finished %d ops%30s\r", done, "");
            }
        }

        void addBytes(long n)
        {
            bytes += n;
        }

        void report(String name)
        {

            // Pretend at least one op was done in case we are running a benchmark
            // that does nto call FinishedSingleOp().
            if (done < 1) {
                done = 1;
            }

            if (bytes > 0) {
                String rate = String.format("%6.1f MB/s", (bytes / 1048576.0) / seconds);
                message.insert(0, " ").insert(0, rate);
            }

            System.out.printf("%-12s : %11.5f micros/op;%s%s\n",
                    name,
                    seconds * 1.0e6 / done,
                    (message == null ? "" : " "),
                    message);
            if (flags.get(Flag.histogram).equals(true)) {
                System.out.printf("Microseconds per op:\n%s\n", hist.toString());
            }
        }
    }
}
