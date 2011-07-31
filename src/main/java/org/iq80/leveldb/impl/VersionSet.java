package org.iq80.leveldb.impl;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Multimap;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.iq80.leveldb.SeekingIterator;
import org.iq80.leveldb.table.Options;
import org.iq80.leveldb.SeekingIterable;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static org.iq80.leveldb.impl.DbConstants.NUM_LEVELS;
import static org.iq80.leveldb.impl.LogMonitors.throwExceptionMonitor;

public class VersionSet implements SeekingIterable<InternalKey, ChannelBuffer>
{
    private static final int L0_COMPACTION_TRIGGER = 4;

    private static final int TARGET_FILE_SIZE = 2 * 1048576;

    // Maximum bytes of overlaps in grandparent (i.e., level+2) before we
    // stop building a single file in a level.level+1 compaction.
    private static final long MAX_GRAND_PARENT_OVERLAP_BYTES = 10 * TARGET_FILE_SIZE;


    private AtomicLong nextFileNumber = new AtomicLong(2);
    private long manifestFileNumber;
    private Version current;
    private long lastSequence;
    private long logNumber;
    private long prevLogNumber;

    private final Map<Version, Object> activeVersions = new MapMaker().weakKeys().<Version, Object>makeMap();
    private final File databaseDir;
    private final TableCache tableCache;
    private final InternalKeyComparator internalKeyComparator;

    private FileChannel descriptorFile;
    private LogWriter descriptorLog;
    private final Multimap<Integer, InternalKey> compactPointers = ArrayListMultimap.create();

    public VersionSet(Options options, File databaseDir, TableCache tableCache, InternalKeyComparator internalKeyComparator)
    {
        this.databaseDir = databaseDir;
        this.tableCache = tableCache;
        this.internalKeyComparator = internalKeyComparator;
        appendVersion(new Version(NUM_LEVELS, tableCache, internalKeyComparator));
    }

    private void appendVersion(Version version)
    {
        Preconditions.checkNotNull(version, "version is null");
        Preconditions.checkArgument(version != current, "version is null");

        current = version;
        activeVersions.put(version, new Object());
    }

    public Version getCurrent()
    {
        return current;
    }

    public long getManifestFileNumber()
    {
        return manifestFileNumber;
    }

    public long getNextFileNumber()
    {
        return nextFileNumber.getAndIncrement();
    }

    public long getLogNumber()
    {
        return logNumber;
    }

    public long getPrevLogNumber()
    {
        return prevLogNumber;
    }

    @Override
    public SeekingIterator<InternalKey, ChannelBuffer> iterator()
    {
        return current.iterator();
    }

    public List<SeekingIterator<InternalKey, ChannelBuffer>> getIterators()
    {
        return current.getIterators();
    }

    public LookupResult get(LookupKey key)
    {
        return current.get(key);
    }

    public boolean overlapInLevel(int level, ChannelBuffer smallestUserKey, ChannelBuffer largestUserKey)
    {
        return current.overlapInLevel(level, smallestUserKey, largestUserKey);
    }

    public int numberOfFilesInLevel(int level)
    {
        return current.numberOfFilesInLevel(level);
    }

    public long numberOfBytesInLevel(int level)
    {
        return current.numberOfFilesInLevel(level);
    }

    public long getLastSequence()
    {
        return lastSequence;
    }

    public void setLastSequence(long newLastSequence)
    {
        Preconditions.checkArgument(newLastSequence >= lastSequence, "Expected newLastSequence to be greater than or equal to current lastSequence");
        this.lastSequence = newLastSequence;
    }

    public void logAndApply(VersionEdit edit)
            throws IOException
    {
        if (edit.getLogNumber() != null) {
            Preconditions.checkArgument(edit.getLogNumber() >= logNumber);
            Preconditions.checkArgument(edit.getLogNumber() < nextFileNumber.get());
        }
        else {
            edit.setLogNumber(logNumber);
        }

        if (edit.getPreviousLogNumber() == null) {
            edit.setPreviousLogNumber(prevLogNumber);
        }

        edit.setNextFileNumber(nextFileNumber.get());
        edit.setLastSequenceNumber(lastSequence);

        Version version = new Version(NUM_LEVELS, tableCache, internalKeyComparator);
        Builder builder = new Builder(this, current);
        builder.apply(edit);
        builder.saveTo(version);

        finalizeVersion(version);

        File newManifestFile = null;
        try {
            // Initialize new descriptor log file if necessary by creating
            // a temporary file that contains a snapshot of the current version.
            if (descriptorLog == null) {
                Preconditions.checkState(descriptorFile == null);
                newManifestFile = new File(databaseDir, Filename.descriptorFileName(manifestFileNumber));
                edit.setNextFileNumber(nextFileNumber.get());
                descriptorFile = new FileOutputStream(newManifestFile).getChannel();
                descriptorLog = new LogWriter(descriptorFile);
                writeSnapshot(descriptorLog);
            }

            // Write new record to MANIFEST log
            ChannelBuffer record = ChannelBuffers.dynamicBuffer();
            edit.encodeTo(record);
            descriptorLog.addRecord(record);
            descriptorFile.force(false);

            // If we just created a new descriptor file, install it by writing a
            // new CURRENT file that points to it.
            if (newManifestFile != null) {
                Filename.setCurrentFile(databaseDir, manifestFileNumber);
            }
        }
        catch (IOException e) {
            // New manifest file was not installed, so clean up state and delete the file
            if (newManifestFile != null) {
                Closeables.closeQuietly(descriptorFile);
                descriptorLog = null;
                descriptorFile = null;

                newManifestFile.delete();
            }
            throw e;
        }

        // Install the new version
        appendVersion(version);
        logNumber = edit.getLogNumber();
        prevLogNumber = edit.getPreviousLogNumber();
    }

    private void writeSnapshot(LogWriter log)
            throws IOException
    {
        // Save metadata
        VersionEdit edit = new VersionEdit();
        // todo implement user provided comparators
        edit.setComparatorName(internalKeyComparator.getClass().getName());

        // Save compaction pointers
        edit.setCompactPointers(compactPointers);

        // Save files
        edit.addFiles(current.getFiles());


        ChannelBuffer record = ChannelBuffers.dynamicBuffer();
        edit.encodeTo(record);
        log.addRecord(record);
    }

    public void recover()
            throws IOException
    {

        // Read "CURRENT" file, which contains a pointer to the current manifest file
        File currentFile = new File(databaseDir, Filename.currentFileName());
        if (!currentFile.exists()) {
            // empty database
            return;
        }

        String currentName = Files.toString(currentFile, Charsets.UTF_8);
        if (currentName.isEmpty() || currentName.charAt(currentName.length() - 1) != '\n') {
            throw new IllegalStateException("CURRENT file does not end with newline");
        }
        currentName = currentName.substring(0, currentName.length() - 1);

        // open file channel
        FileChannel fileChannel = new FileInputStream(new File(databaseDir, currentName)).getChannel();

        // read log edit log
        Long nextFileNumber = null;
        Long lastSequence = null;
        Long logNumber = null;
        Long prevLogNumber = null;
        Builder builder = new Builder(this, current);

        LogReader reader = new LogReader(fileChannel, throwExceptionMonitor(), true, 0);
        for (ChannelBuffer record = reader.readRecord(); record != null; record = reader.readRecord()) {
            // read version edit
            VersionEdit edit = new VersionEdit(record);

            // verify comparator
            // todo implement user comparator
            String editComparator = edit.getComparatorName();
            String userComparator = internalKeyComparator.getClass().getName();
            Preconditions.checkArgument(editComparator != null && !editComparator.equals(userComparator),
                    "Expected user comparator %s to match existing database comparator ", userComparator, editComparator);

            // apply edit
            builder.apply(edit);

            // save edit values for verification below
            logNumber = coalesce(logNumber, edit.getLogNumber());
            prevLogNumber = coalesce(prevLogNumber, edit.getPreviousLogNumber());
            nextFileNumber = coalesce(nextFileNumber, edit.getNextFileNumber());
            lastSequence = coalesce(lastSequence, edit.getLastSequenceNumber());
        }

        List<String> problems = newArrayList();
        if (nextFileNumber == null) {
            problems.add("Descriptor does not contain a meta-nextfile entry");
        }
        if (logNumber == null) {
            problems.add("Descriptor does not contain a meta-lognumber entry");
        }
        if (lastSequence == null) {
            problems.add("Descriptor does not contain a last-sequence-number entry");
        }
        if (!problems.isEmpty()) {
            throw new RuntimeException("Corruption: \n\t" + Joiner.on("\n\t").join(problems));
        }

        if (prevLogNumber == null) {
            prevLogNumber = 0L;
        }

        Version newVersion = new Version(NUM_LEVELS, tableCache, internalKeyComparator);
        builder.saveTo(newVersion);

        // Install recovered version
        finalizeVersion(newVersion);

        appendVersion(newVersion);
        manifestFileNumber = nextFileNumber;
        this.nextFileNumber.set(nextFileNumber + 1);
        this.lastSequence = lastSequence;
        this.logNumber = logNumber;
        this.prevLogNumber = prevLogNumber;
    }

    private void finalizeVersion(Version version)
    {
        // Precomputed best level for next compaction
        int bestLevel = -1;
        double bestScore = -1;

        for (int level = 0; level < version.numberOfLevels() - 1; level++) {
            double score;
            if (level == 0) {
                // We treat level-0 specially by bounding the number of files
                // instead of number of bytes for two reasons:
                //
                // (1) With larger write-buffer sizes, it is nice not to do too
                // many level-0 compactions.
                //
                // (2) The files in level-0 are merged on every read and
                // therefore we wish to avoid too many files when the individual
                // file size is small (perhaps because of a small write-buffer
                // setting, or very high compression ratios, or lots of
                // overwrites/deletions).
                score = 1.0 * version.numberOfFilesInLevel(level) / L0_COMPACTION_TRIGGER;
            }
            else {
                // Compute the ratio of current size to size limit.
                long levelBytes = 0;
                for (FileMetaData fileMetaData : version.getFiles(level)) {
                    levelBytes += fileMetaData.getFileSize();
                }
                score = 1.0 * levelBytes / maxBytesForLevel(level);
            }

            if (score > bestScore) {
                bestLevel = level;
                bestScore = score;
            }
        }

        version.setCompactionLevel(bestLevel);
        version.setCompactionScore(bestScore);
    }

    private static <V> V coalesce(V... values)
    {
        for (V value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    public List<FileMetaData> getLiveFiles()
    {
        ImmutableList.Builder<FileMetaData> builder = ImmutableList.builder();
        for (Version activeVersion : activeVersions.keySet()) {
            builder.addAll(activeVersion.getFiles().values());
        }
        return builder.build();
    }


    private static double maxBytesForLevel(int level)
    {
        // Note: the result for level zero is not really used since we set
        // the level-0 compaction threshold based on number of files.
        double result = 10 * 1048576.0;  // Result for both level-0 and level-1
        while (level > 1) {
            result *= 10;
            level--;
        }
        return result;
    }

    private static long maxFileSizeForLevel(int level)
    {
        return TARGET_FILE_SIZE;  // We could vary per level to reduce number of files?
    }

    public boolean needsCompaction()
    {
        // todo
        return false;  //To change body of created methods use File | Settings | File Templates.
    }


    /**
     * A helper class so we can efficiently apply a whole sequence
     * of edits to a particular state without creating intermediate
     * Versions that contain full copies of the intermediate state.
     */
    private static class Builder
    {
        private final VersionSet versionSet;
        private final Version baseVersion;
        private final List<LevelState> levels;

        private Builder(VersionSet versionSet, Version baseVersion)
        {
            this.versionSet = versionSet;
            this.baseVersion = baseVersion;

            levels = newArrayListWithCapacity(baseVersion.numberOfLevels());
            for (int i = 0; i < baseVersion.numberOfLevels(); i++) {
                levels.add(new LevelState(versionSet.internalKeyComparator));
            }
        }

        /**
         * Apply the specified edit to the current state.
         */
        public void apply(VersionEdit edit)
        {
            // Update compaction pointers
            for (Entry<Integer, InternalKey> entry : edit.getCompactPointers().entries()) {
                Integer level = entry.getKey();
                InternalKey internalKey = entry.getValue();
                versionSet.compactPointers.put(level, internalKey);
            }

            // Delete files
            for (Entry<Integer, Long> entry : edit.getDeletedFiles().entries()) {
                Integer level = entry.getKey();
                Long fileNumber = entry.getValue();
                levels.get(level).deletedFiles.add(fileNumber);
                // todo missing update to addedFiles?
            }

            // Add new files
            for (Entry<Integer, FileMetaData> entry : edit.getNewFiles().entries()) {
                Integer level = entry.getKey();
                FileMetaData fileMetaData = entry.getValue();

                // We arrange to automatically compact this file after
                // a certain number of seeks.  Let's assume:
                //   (1) One seek costs 10ms
                //   (2) Writing or reading 1MB costs 10ms (100MB/s)
                //   (3) A compaction of 1MB does 25MB of IO:
                //         1MB read from this level
                //         10-12MB read from next level (boundaries may be misaligned)
                //         10-12MB written to next level
                // This implies that 25 seeks cost the same as the compaction
                // of 1MB of data.  I.e., one seek costs approximately the
                // same as the compaction of 40KB of data.  We are a little
                // conservative and allow approximately one seek for every 16KB
                // of data before triggering a compaction.
                int allowedSeeks = (int) (fileMetaData.getFileSize() / 16384);
                if (allowedSeeks < 100) {
                    allowedSeeks = 100;
                }
                fileMetaData.setAllowedSeeks(allowedSeeks);

                levels.get(level).deletedFiles.remove(fileMetaData.getNumber());
                levels.get(level).addedFiles.add(fileMetaData);
            }
        }

        /**
         * Saves the current state in specified version.
         */
        public void saveTo(Version version)
        {
            FileMetaDataBySmallestKey cmp = new FileMetaDataBySmallestKey(versionSet.internalKeyComparator);
            for (int level = 0; level < baseVersion.numberOfLevels(); level++) {

                // Merge the set of added files with the set of pre-existing files.
                // Drop any deleted files.  Store the result in *v.

                Collection<FileMetaData> baseFiles = baseVersion.getFiles().values();
                SortedSet<FileMetaData> addedFiles = levels.get(level).addedFiles;

                // files must be added in sorted order to assertion check in maybeAddFile works
                ArrayList<FileMetaData> sortedFiles = newArrayListWithCapacity(baseFiles.size() + addedFiles.size());
                sortedFiles.addAll(baseFiles);
                sortedFiles.addAll(addedFiles);
                Collections.sort(sortedFiles, cmp);

                for (FileMetaData fileMetaData : sortedFiles) {
                    maybeAddFile(version, level, fileMetaData);
                }

                //#ifndef NDEBUG  todo
                // Make sure there is no overlap in levels > 0
                if (level > 0) {
                    long previousFileNumber = 0;
                    InternalKey previousEnd = null;
                    for (FileMetaData fileMetaData : version.getFiles().values()) {
                        if (previousEnd != null) {
                            Preconditions.checkArgument(versionSet.internalKeyComparator.compare(
                                    previousEnd,
                                    fileMetaData.getSmallest()
                            ) < 0, "Overlapping files %s and %s in level %s", previousFileNumber, fileMetaData.getNumber(), level);
                        }

                        previousFileNumber = fileMetaData.getNumber();
                        previousEnd = fileMetaData.getLargest();
                    }
                }
                //#endif
            }
        }

        public void maybeAddFile(Version version, int level, FileMetaData fileMetaData)
        {
            if (levels.get(level).deletedFiles.contains(fileMetaData.getNumber())) {
                // File is deleted: do nothing
            }
            else {
                List<FileMetaData> files = version.getFiles(level);
                if (level > 0 && !files.isEmpty()) {
                    // Must not overlap
                    Preconditions.checkArgument(versionSet.internalKeyComparator.compare(
                            files.get(files.size() - 1).getLargest(),
                            fileMetaData.getSmallest()
                    ) < 0, "new file overlaps existing files in range");
                }
                version.addFile(level, fileMetaData);
            }
        }

        private static class FileMetaDataBySmallestKey implements Comparator<FileMetaData>
        {
            private final InternalKeyComparator internalKeyComparator;

            private FileMetaDataBySmallestKey(InternalKeyComparator internalKeyComparator)
            {
                this.internalKeyComparator = internalKeyComparator;
            }

            @Override
            public int compare(FileMetaData f1, FileMetaData f2)
            {
                return ComparisonChain
                        .start()
                        .compare(f1.getSmallest(), f2.getSmallest(), internalKeyComparator)
                        .compare(f1.getNumber(), f2.getNumber())
                        .result();
            }
        }

        private static class LevelState
        {
            private final SortedSet<FileMetaData> addedFiles;
            private final Set<Long> deletedFiles = new HashSet<Long>();

            public LevelState(InternalKeyComparator internalKeyComparator)
            {
                addedFiles = new TreeSet<FileMetaData>(new FileMetaDataBySmallestKey(internalKeyComparator));
            }

            @Override
            public String toString()
            {
                final StringBuilder sb = new StringBuilder();
                sb.append("LevelState");
                sb.append("{addedFiles=").append(addedFiles);
                sb.append(", deletedFiles=").append(deletedFiles);
                sb.append('}');
                return sb.toString();
            }
        }
    }
}
