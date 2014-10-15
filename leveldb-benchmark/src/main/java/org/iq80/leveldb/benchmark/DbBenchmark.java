/**
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

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.impl.DbImpl;
import org.iq80.leveldb.util.Closeables;
import org.iq80.leveldb.util.FileUtils;
import org.iq80.leveldb.util.PureJavaCrc32C;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.SliceOutput;
import org.iq80.leveldb.util.Slices;
import org.iq80.leveldb.util.Snappy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Charsets.UTF_8;
import static org.iq80.leveldb.benchmark.DbBenchmark.DBState.EXISTING;
import static org.iq80.leveldb.benchmark.DbBenchmark.DBState.FRESH;
import static org.iq80.leveldb.benchmark.DbBenchmark.Order.RANDOM;
import static org.iq80.leveldb.benchmark.DbBenchmark.Order.SEQUENTIAL;
import static org.iq80.leveldb.impl.DbConstants.NUM_LEVELS;

public class DbBenchmark
{
    private boolean useExisting;
    private Integer writeBufferSize;
    private File databaseDir;
    private double compressionRatio;
    private long startTime;

    enum Order
    {
        SEQUENTIAL,
        RANDOM
    }

    enum DBState
    {
        FRESH,
        EXISTING
    }

    //    Cache cache_;
    private List<String> benchmarks;
    private DB db_;
    private final int num_;
    private int reads_;
    private final int valueSize;
    private int heap_counter_;
    private double last_op_finish_;
    private long bytes_;
    private String message_;
    private String post_message_;
    //    private Histogram hist_;
    private RandomGenerator gen_;
    private final Random rand_;

    // State kept for progress messages
    int done_;
    int next_report_;     // When to report next

    final DBFactory factory;

    public DbBenchmark(Map<Flag, Object> flags) throws Exception
    {
        ClassLoader cl = DbBenchmark.class.getClassLoader();
        factory = (DBFactory) cl.loadClass(System.getProperty("leveldb.factory", "org.iq80.leveldb.impl.Iq80DBFactory")).newInstance();
        benchmarks = (List<String>) flags.get(Flag.benchmarks);
        num_ = (Integer) flags.get(Flag.num);
        reads_ = (Integer) (flags.get(Flag.reads) == null ? flags.get(Flag.num) : flags.get(Flag.reads));
        valueSize = (Integer) flags.get(Flag.value_size);
        writeBufferSize = (Integer) flags.get(Flag.write_buffer_size);
        compressionRatio = (Double) flags.get(Flag.compression_ratio);
        useExisting = (Boolean) flags.get(Flag.use_existing_db);
        heap_counter_ = 0;
        bytes_ = 0;
        rand_ = new Random(301);


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

        gen_ = new RandomGenerator(compressionRatio);
    }

    private void run()
            throws IOException
    {
        printHeader();
        open();

        for (String benchmark : benchmarks) {
            start();

            boolean known = true;

            if (benchmark.equals("fillseq")) {
                write(new WriteOptions(), SEQUENTIAL, FRESH, num_, valueSize, 1);
            }
            else if (benchmark.equals("fillbatch")) {
                write(new WriteOptions(), SEQUENTIAL, FRESH, num_, valueSize, 1000);
            }
            else if (benchmark.equals("fillrandom")) {
                write(new WriteOptions(), RANDOM, FRESH, num_, valueSize, 1);
            }
            else if (benchmark.equals("overwrite")) {
                write(new WriteOptions(), RANDOM, EXISTING, num_, valueSize, 1);
            }
            else if (benchmark.equals("fillsync")) {
                write(new WriteOptions().sync(true), RANDOM, FRESH, num_ / 1000, valueSize, 1);
            }
            else if (benchmark.equals("fill100K")) {
                write(new WriteOptions(), RANDOM, FRESH, num_ / 1000, 100 * 1000, 1);
            }
            else if (benchmark.equals("readseq")) {
                readSequential();
            }
            else if (benchmark.equals("readreverse")) {
                readReverse();
            }
            else if (benchmark.equals("readrandom")) {
                readRandom();
            }
            else if (benchmark.equals("readhot")) {
                readHot();
            }
            else if (benchmark.equals("readrandomsmall")) {
                int n = reads_;
                reads_ /= 1000;
                readRandom();
                reads_ = n;
            }
            else if (benchmark.equals("compact")) {
                compact();
            }
            else if (benchmark.equals("crc32c")) {
                crc32c(4096, "(4k per op)");
            }
            else if (benchmark.equals("acquireload")) {
                acquireLoad();
            }
            else if (benchmark.equals("snappycomp")) {
                if( Snappy.available() ) {
                    snappyCompress();
                }
            }
            else if (benchmark.equals("snappyuncomp")) {
                if( Snappy.available() ) {
                    snappyUncompressDirectBuffer();
                }
            }
            else if (benchmark.equals("unsnap-array")) {
                if( Snappy.available() ) {
                    snappyUncompressArray();
                }
            }
            else if (benchmark.equals("unsnap-direct")) {
                if( Snappy.available() ) {
                    snappyUncompressDirectBuffer();
                }
            }
            else if (benchmark.equals("heapprofile")) {
                heapProfile();
            }
            else if (benchmark.equals("stats")) {
                printStats();
            }
            else {
                known = false;
                System.err.println("Unknown benchmark: " + benchmark);
            }
            if (known) {
                stop(benchmark);
            }
        }
        db_.close();
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
        System.out.printf("Entries:    %d\n", num_);
        System.out.printf("RawSize:    %.1f MB (estimated)\n",
                ((kKeySize + valueSize) * num_) / 1048576.0);
        System.out.printf("FileSize:   %.1f MB (estimated)\n",
                (((kKeySize + valueSize * compressionRatio) * num_)
                        / 1048576.0));
        printWarnings();
        System.out.printf("------------------------------------------------\n");
    }

    void printWarnings()
    {
        boolean assertsEnabled = false;
        assert assertsEnabled = true; // Intentional side effect!!!
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
        // todo block cache
        if (writeBufferSize != null) {
            options.writeBufferSize(writeBufferSize);
        }
        db_ = factory.open(databaseDir, options);
    }

    private void start()
    {
        startTime = System.nanoTime();
        bytes_ = 0;
        message_ = null;
        last_op_finish_ = startTime;
        // hist.clear();
        done_ = 0;
        next_report_ = 100;
    }

    private void stop(String benchmark)
    {
        long endTime = System.nanoTime();
        double elapsedSeconds = 1.0d * (endTime - startTime) / TimeUnit.SECONDS.toNanos(1);

        // Pretend at least one op was done in case we are running a benchmark
        // that does nto call FinishedSingleOp().
        if (done_ < 1) {
            done_ = 1;
        }

        if (bytes_ > 0) {
            String rate = String.format("%6.1f MB/s", (bytes_ / 1048576.0) / elapsedSeconds);
            if (message_ != null) {
                message_ = rate + " " + message_;
            }
            else {
                message_ = rate;
            }
        }
        else if (message_ == null) {
            message_ = "";
        }

        System.out.printf("%-12s : %11.5f micros/op;%s%s\n",
                benchmark,
                elapsedSeconds * 1e6 / done_,
                (message_ == null ? "" : " "),
                message_);
//        if (FLAGS_histogram) {
//            System.out.printf("Microseconds per op:\n%s\n", hist_.ToString().c_str());
//        }

        if (post_message_ != null) {
            System.out.printf("\n%s\n", post_message_);
            post_message_ = null;
        }

    }

    private void write(WriteOptions writeOptions, Order order, DBState state, int numEntries, int valueSize, int entries_per_batch)
            throws IOException
    {
        if (state == FRESH) {
            if (useExisting) {
                message_ = "skipping (--use_existing_db is true)";
                return;
            }
            db_.close();
            db_ = null;
            destroyDb();
            open();
            start(); // Do not count time taken to destroy/open
        }

        if (numEntries != num_) {
            message_ = String.format("(%d ops)", numEntries);
        }

        for (int i = 0; i < numEntries; i += entries_per_batch) {
            WriteBatch batch = db_.createWriteBatch();
            for (int j = 0; j < entries_per_batch; j++) {
                int k = (order == SEQUENTIAL) ? i + j : rand_.nextInt(num_);
                byte[] key = formatNumber(k);
                batch.put(key, gen_.generate(valueSize));
                bytes_ += valueSize + key.length;
                finishedSingleOp();
            }
            db_.write(batch, writeOptions);
            batch.close();
        }
    }

    public static byte[] formatNumber(long n)
    {
        Preconditions.checkArgument(n >= 0, "number must be positive");

        byte []slice = new byte[16];

        int i = 15;
        while (n > 0) {
            slice[i--] = (byte) ('0' + (n % 10));
            n /= 10;
        }
        while (i >= 0) {
            slice[i--] = '0';
        }
        return slice;
    }

    private void finishedSingleOp()
    {
//        if (histogram) {
//            todo
//        }
        done_++;
        if (done_ >= next_report_) {
            if (next_report_ < 1000) {
                next_report_ += 100;
            }
            else if (next_report_ < 5000) {
                next_report_ += 500;
            }
            else if (next_report_ < 10000) {
                next_report_ += 1000;
            }
            else if (next_report_ < 50000) {
                next_report_ += 5000;
            }
            else if (next_report_ < 100000) {
                next_report_ += 10000;
            }
            else if (next_report_ < 500000) {
                next_report_ += 50000;
            }
            else {
                next_report_ += 100000;
            }
            System.out.printf("... finished %d ops%30s\r", done_, "");

        }
    }

    private void readSequential()
    {
        for (int loops = 0; loops < 5; loops++) {
            DBIterator iterator = db_.iterator();
            iterator.seekToFirst();
            for (int i = 0; i < reads_ && iterator.hasNext(); i++) {
                Map.Entry<byte[], byte[]> entry = iterator.next();
                bytes_ += entry.getKey().length + entry.getValue().length;
                finishedSingleOp();
            }
            Closeables.closeQuietly(iterator);
        }
    }

    private void readReverse()
    {
        for (int loops = 0; loops < 3; loops++) {
            DBIterator iterator = db_.iterator();
            iterator.seekToLast();
            for (int i = 0; i < reads_ && iterator.hasPrev(); i++) {
                Map.Entry<byte[], byte[]> entry = iterator.prev();
                bytes_ += entry.getKey().length + entry.getValue().length;
                finishedSingleOp();
            }
            Closeables.closeQuietly(iterator);
        }
    }

    private void readRandom()
    {
        for (int i = 0; i < reads_; i++) {
            byte[] key = formatNumber(rand_.nextInt(num_));
            byte[] value = db_.get(key);
            Preconditions.checkNotNull(value, "db.get(%s) is null", new String(key, UTF_8));
            bytes_ += key.length + value.length;
            finishedSingleOp();
        }
    }

    private void readHot()
    {
        int range = (num_ + 99) / 100;
        for (int i = 0; i < reads_; i++) {
            byte[] key = formatNumber(rand_.nextInt(range));
            byte[] value = db_.get(key);
            bytes_ += key.length + value.length;
            finishedSingleOp();
        }
    }

    private void compact()
            throws IOException
    {
        if(db_ instanceof DbImpl) {
            ((DbImpl)db_).compactMemTable();
            for (int level = 0; level < NUM_LEVELS - 1; level++) {
                ((DbImpl)db_).compactRange(level, Slices.copiedBuffer("", UTF_8), Slices.copiedBuffer("~", UTF_8));
            }
        }
    }

    private void crc32c(int blockSize, String message)
    {
        // Checksum about 500MB of data total
        byte[] data = new byte[blockSize];
        for (int i = 0; i < data.length; i++) {
            data[i] = 'x';

        }

        long bytes = 0;
        int crc = 0;
        while (bytes < 1000 * 1048576) {
            PureJavaCrc32C checksum = new PureJavaCrc32C();
            checksum.update(data, 0, blockSize);
            crc = checksum.getMaskedValue();
            finishedSingleOp();
            bytes += blockSize;
        }
        System.out.printf("... crc=0x%x\r", crc);

        bytes_ = bytes;
        // Print so result is not dead
        message_ = message;
    }

    private void acquireLoad()
    {
        //To change body of created methods use File | Settings | File Templates.
    }

    private void snappyCompress()
    {
        byte[] raw = gen_.generate(new Options().blockSize());
        byte[] compressedOutput = new byte[Snappy.maxCompressedLength(raw.length)];

        long produced = 0;

        // attempt to compress the block
        while (bytes_ < 1024 * 1048576) {  // Compress 1G
            try {
                int compressedSize = Snappy.compress(raw, 0, raw.length, compressedOutput, 0);
                bytes_ += raw.length;
                produced += compressedSize;
            }
            catch (IOException ignored) {
                throw Throwables.propagate(ignored);
            }

            finishedSingleOp();
        }

        message_ = String.format("(output: %.1f%%)", (produced * 100.0) / bytes_);
    }

    private void snappyUncompressArray()
    {
        int inputSize = new Options().blockSize();
        byte[] compressedOutput = new byte[Snappy.maxCompressedLength(inputSize)];
        byte raw[] = gen_.generate(inputSize);
        int compressedLength;
        try {
            compressedLength = Snappy.compress(raw, 0, raw.length, compressedOutput, 0);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        // attempt to uncompress the block
        while (bytes_ < 5L * 1024 * 1048576) {  // Compress 1G
            try {
                Snappy.uncompress(compressedOutput, 0, compressedLength, raw, 0);
                bytes_ += inputSize;
            }
            catch (IOException ignored) {
                throw Throwables.propagate(ignored);
            }

            finishedSingleOp();
        }
    }

    private void snappyUncompressDirectBuffer()
    {
        int inputSize = new Options().blockSize();
        byte[] compressedOutput = new byte[Snappy.maxCompressedLength(inputSize)];
        byte raw[] = gen_.generate(inputSize);
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

        // attempt to uncompress the block
        while (bytes_ < 5L * 1024 * 1048576) {  // Compress 1G
            try {
                uncompressedBuffer.clear();
                compressedBuffer.position(0);
                compressedBuffer.limit(compressedLength);
                Snappy.uncompress(compressedBuffer, uncompressedBuffer);
                bytes_ += inputSize;
            }
            catch (IOException ignored) {
                throw Throwables.propagate(ignored);
            }

            finishedSingleOp();
        }
    }

    private void heapProfile()
    {
        //To change body of created methods use File | Settings | File Templates.
    }

    private void destroyDb()
    {
        Closeables.closeQuietly(db_);
        db_ = null;
        FileUtils.deleteRecursively(databaseDir);
    }

    private void printStats()
    {
        //To change body of created methods use File | Settings | File Templates.
    }

    public static void main(String[] args)
            throws Exception
    {
        Map<Flag, Object> flags = new EnumMap<Flag, Object>(Flag.class);
        for (Flag flag : Flag.values()) {
            flags.put(flag, flag.getDefaultValue());
        }
        for (String arg : args) {
            boolean valid = false;
            if (arg.startsWith("--")) {
                try {
                    ImmutableList<String> parts = ImmutableList.copyOf(Splitter.on("=").limit(2).split(arg.substring(2)));
                    if (parts.size() != 2) {

                    }
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
        benchmarks(ImmutableList.<String>of(
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
        ))
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return ImmutableList.copyOf(Splitter.on(",").trimResults().omitEmptyStrings().split(value));
                    }
                },

        // Arrange to generate values that shrink to this fraction of
        // their original size after compression
        compression_ratio(0.5d)
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return Double.parseDouble(value);
                    }
                },

        // Print histogram of operation timings
        histogram(false)
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return Boolean.parseBoolean(value);
                    }
                },

        // If true, do not destroy the existing database.  If you set this
        // flag and also specify a benchmark that wants a fresh database, that
        // benchmark will fail.
        use_existing_db(false)
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return Boolean.parseBoolean(value);
                    }
                },

        // Number of key/values to place in database
        num(1000000)
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return Integer.parseInt(value);
                    }
                },

        // Number of read operations to do.  If negative, do FLAGS_num reads.
        reads(null)
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return Integer.parseInt(value);
                    }
                },

        // Size of each value
        value_size(100)
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return Integer.parseInt(value);
                    }
                },

        // Number of bytes to buffer in memtable before compacting
        // (initialized to default value by "main")
        write_buffer_size(null)
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return Integer.parseInt(value);
                    }
                },

        // Number of bytes to use as a cache of uncompressed data.
        // Negative means use default settings.
        cache_size(-1)
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return Integer.parseInt(value);
                    }
                },

        // Maximum number of files to keep open at the same time (use default if == 0)
        open_files(0)
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return Integer.parseInt(value);
                    }
                },

        // Use the db with the following name.
        db("/tmp/dbbench")
                {
                    @Override
                    public Object parseValue(String value)
                    {
                        return value;
                    }
                },;

        private final Object defaultValue;

        private Flag(Object defaultValue)
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
            sliceOutput.writeByte((byte) (' ' + random.nextInt(95)));
        }
        return rawData;
    }
}
