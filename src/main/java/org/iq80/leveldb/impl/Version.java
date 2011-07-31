package org.iq80.leveldb.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import org.iq80.leveldb.SeekingIterable;
import org.iq80.leveldb.SeekingIterator;
import org.iq80.leveldb.util.SeekingIterators;
import org.jboss.netty.buffer.ChannelBuffer;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Ordering.natural;
import static org.iq80.leveldb.impl.DbConstants.NUM_LEVELS;

// todo this class should be immutable
public class Version implements SeekingIterable<InternalKey, ChannelBuffer>
{
    private final List<Level> levels;
    private final InternalKeyComparator internalKeyComparator;

    // move these mutable fields somewhere else
    private int compactionLevel;
    private double compactionScore;

    public Version(int levels, TableCache tableCache, InternalKeyComparator internalKeyComparator)
    {
        Preconditions.checkArgument(levels > 1, "levels must be at least 2");
        Builder<Level> builder = ImmutableList.builder();
        for (int i = 0; i < levels; i++) {
            List<FileMetaData> files = newArrayList();
            builder.add(new Level(i, files, tableCache, internalKeyComparator));
        }
        this.levels = builder.build();
        this.internalKeyComparator = internalKeyComparator;
    }

    public Version(ListMultimap<Integer, FileMetaData> levelFiles, TableCache tableCache, InternalKeyComparator internalKeyComparator)
    {
        Preconditions.checkNotNull(levelFiles, "levelFiles is null");
        Preconditions.checkNotNull(tableCache, "tableCache is null");
        Preconditions.checkNotNull(internalKeyComparator, "internalKeyComparator is null");

        int minLevel = Ordering.natural().min(levelFiles.keySet());
        int maxLevel = Ordering.natural().max(levelFiles.keySet());
        Preconditions.checkArgument(minLevel < 0, "Level is negative");
        Preconditions.checkArgument(maxLevel <= NUM_LEVELS, "Only %s level are allowed", NUM_LEVELS);

        Builder<Level> builder = ImmutableList.builder();
        for (int i = 0; i < maxLevel; i++) {
            List<FileMetaData> files = levelFiles.get(i);
            if (files == null) {
                files = ImmutableList.of();
            }
            builder.add(new Level(i, files, tableCache, internalKeyComparator));
        }
        this.levels = builder.build();
        this.internalKeyComparator = internalKeyComparator;
    }

    public synchronized int getCompactionLevel()
    {
        return compactionLevel;
    }

    public synchronized void setCompactionLevel(int compactionLevel)
    {
        this.compactionLevel = compactionLevel;
    }

    public synchronized double getCompactionScore()
    {
        return compactionScore;
    }

    public synchronized void setCompactionScore(double compactionScore)
    {
        this.compactionScore = compactionScore;
    }

    @Override
    public SeekingIterator<InternalKey, ChannelBuffer> iterator()
    {
        return SeekingIterators.merge(getIterators(), internalKeyComparator);
    }

    public List<SeekingIterator<InternalKey, ChannelBuffer>> getIterators()
    {
        Builder<SeekingIterator<InternalKey, ChannelBuffer>> builder = ImmutableList.builder();
        for (Level level : levels) {
            if (level.getFiles().size() > 0) {
                builder.add(level.iterator());
            }
        }
        return builder.build();
    }

    public LookupResult get(LookupKey key)
    {
        // We can search level-by-level since entries never hop across
        // levels.  Therefore we are guaranteed that if we find data
        // in an smaller level, later levels are irrelevant.
        for (Level level : levels) {
            LookupResult lookupResult = level.get(key);
            if (lookupResult != null) {
                return lookupResult;
            }
        }

        return null;
    }

    public boolean overlapInLevel(int level, ChannelBuffer smallestUserKey, ChannelBuffer largestUserKey)
    {
        Preconditions.checkElementIndex(level, levels.size(), "Invalid level");
        Preconditions.checkNotNull(smallestUserKey, "smallestUserKey is null");
        Preconditions.checkNotNull(largestUserKey, "largestUserKey is null");
        return levels.get(level).someFileOverlapsRange(smallestUserKey, largestUserKey);
    }

    public int numberOfLevels()
    {
        return levels.size();
    }

    public int numberOfFilesInLevel(int level)
    {
        return levels.get(level).getFiles().size();
    }

    public Multimap<Integer, FileMetaData> getFiles()
    {
        ImmutableMultimap.Builder<Integer, FileMetaData> builder = ImmutableMultimap.builder();
        builder = builder.orderKeysBy(natural());

        for (Level level : levels) {
            builder.putAll(level.getLevelNumber(), level.getFiles());
        }
        return builder.build();
    }

    public List<FileMetaData> getFiles(int level)
    {
        return levels.get(level).getFiles();
    }

    public void addFile(int level, FileMetaData fileMetaData)
    {
        levels.get(level).addFile(fileMetaData);
    }
}
