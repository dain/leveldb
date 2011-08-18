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
package org.iq80.leveldb.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
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
    private FileMetaData fileToCompact;
    private int fileToCompactLevel;
    private final TableCache tableCache;

    public Version(int levels, TableCache tableCache, InternalKeyComparator internalKeyComparator)
    {
        Preconditions.checkArgument(levels > 1, "levels must be at least 2");
        this.tableCache = tableCache;
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

        this.tableCache = tableCache;
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

    public InternalKeyComparator getInternalKeyComparator()
    {
        return internalKeyComparator;
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
        ReadStats readStats = new ReadStats();
        for (Level level : levels) {
            LookupResult lookupResult = level.get(key, readStats);
            if (lookupResult != null) {
                return lookupResult;
            }
        }
        updateStats(readStats.getSeekFileLevel(), readStats.getSeekFile());
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

    private boolean updateStats(int seekFileLevel, FileMetaData seekFile)
    {
        if (seekFile == null) {
            return false;
        }

        seekFile.decrementAllowedSeeks();
        if (seekFile.getAllowedSeeks() <= 0 && fileToCompact == null) {
            fileToCompact = seekFile;
            fileToCompactLevel = seekFileLevel;
            return true;
        }
        return false;
    }

    public FileMetaData getFileToCompact()
    {
        return fileToCompact;
    }

    public int getFileToCompactLevel()
    {
        return fileToCompactLevel;
    }

    public long getApproximateOffsetOf(InternalKey key)
    {
        long result = 0;
        for (int level = 0; level < NUM_LEVELS; level++) {
            for (FileMetaData fileMetaData : getFiles(level)) {
                if (internalKeyComparator.compare(fileMetaData.getLargest(), key) <= 0) {
                    // Entire file is before "ikey", so just add the file size
                    result += fileMetaData.getFileSize();
                }
                else if (internalKeyComparator.compare(fileMetaData.getSmallest(), key) > 0) {
                    // Entire file is after "ikey", so ignore
                    if (level > 0) {
                        // Files other than level 0 are sorted by meta->smallest, so
                        // no further files in this level will contain data for
                        // "ikey".
                        break;
                    }
                }
                else {
                    // "ikey" falls in the range for this table.  Add the
                    // approximate offset of "ikey" within the table.
                    result += tableCache.getApproximateOffsetOf(fileMetaData, key.encode());
                }
            }
        }
        return result;
    }
}
