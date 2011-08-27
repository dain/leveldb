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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.iq80.leveldb.table.UserComparator;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.SeekingIterators;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Lists.newArrayList;
import static org.iq80.leveldb.impl.FileMetaData.GET_LARGEST_USER_KEY;
import static org.iq80.leveldb.impl.SequenceNumber.MAX_SEQUENCE_NUMBER;
import static org.iq80.leveldb.impl.ValueType.VALUE;

// todo this class should be immutable
public class Level implements SeekingIterable<InternalKey, Slice>
{
    private final int levelNumber;
    private final TableCache tableCache;
    private final InternalKeyComparator internalKeyComparator;
    private final List<FileMetaData> files;

    public Level(int levelNumber, List<FileMetaData> files, TableCache tableCache, InternalKeyComparator internalKeyComparator)
    {
        Preconditions.checkArgument(levelNumber >= 0, "levelNumber is negative");
        Preconditions.checkNotNull(files, "files is null");
        Preconditions.checkNotNull(tableCache, "tableCache is null");
        Preconditions.checkNotNull(internalKeyComparator, "internalKeyComparator is null");

        this.files = newArrayList(files);
        this.tableCache = tableCache;
        this.internalKeyComparator = internalKeyComparator;
        Preconditions.checkArgument(levelNumber >= 0, "levelNumber is negative");
        this.levelNumber = levelNumber;
    }

    public int getLevelNumber()
    {
        return levelNumber;
    }

    public List<FileMetaData> getFiles()
    {
        return files;
    }

    @Override
    public SeekingIterator<InternalKey, Slice> iterator()
    {
        if (files.isEmpty()) {
            return SeekingIterators.emptyIterator();
        } else if (files.size() == 1) {
            return tableCache.newIterator(files.get(0));
        } else if (levelNumber == 0) {
            Builder<SeekingIterator<InternalKey, Slice>> builder = ImmutableList.builder();
            for (FileMetaData file : files) {
                builder.add(tableCache.newIterator(file));
            }
            return SeekingIterators.merge(builder.build(), internalKeyComparator);
        }
        else {
            return createLevelConcatIterator(tableCache, files, internalKeyComparator);
        }
    }

    public static SeekingIterator<InternalKey, Slice> createLevelConcatIterator(final TableCache tableCache, List<FileMetaData> files, InternalKeyComparator internalKeyComparator)
    {
        FileMetaDataSeekingIterator fileMetaDataIterator = new FileMetaDataSeekingIterator(files, internalKeyComparator);
        return SeekingIterators.concat(SeekingIterators.transformValues(fileMetaDataIterator, new Function<FileMetaData, SeekingIterator<InternalKey, Slice>>()
        {
            public SeekingIterator<InternalKey, Slice> apply(FileMetaData fileMetaData)
            {
                return tableCache.newIterator(fileMetaData);
            }
        }));
    }

    public LookupResult get(LookupKey key, ReadStats readStats)
    {
        if (files.isEmpty()) {
            return null;
        }

        List<FileMetaData> fileMetaDataList = Lists.newArrayListWithCapacity(files.size());
        if (levelNumber == 0) {
            for (FileMetaData fileMetaData : files) {
                if (internalKeyComparator.getUserComparator().compare(key.getUserKey(), fileMetaData.getSmallest().getUserKey()) >= 0 &&
                        internalKeyComparator.getUserComparator().compare(key.getUserKey(), fileMetaData.getLargest().getUserKey()) <= 0) {
                    fileMetaDataList.add(fileMetaData);
                }
            }
        }
        else {
            // Binary search to find earliest index whose largest key >= ikey.
            int index = ceilingEntryIndex(Lists.transform(files, GET_LARGEST_USER_KEY), key.getInternalKey(), internalKeyComparator);

            // did we find any files that could contain the key?
            if (index >= files.size()) {
                return null;
            }

            // check if the smallest user key in the file is less than the target user key
            FileMetaData fileMetaData = files.get(index);
            if (internalKeyComparator.getUserComparator().compare(key.getUserKey(), fileMetaData.getSmallest().getUserKey()) < 0) {
                return null;
            }

            // search this file
            fileMetaDataList.add(fileMetaData);
        }

        readStats.clear();
        for (FileMetaData fileMetaData : fileMetaDataList) {
            // open the iterator
            SeekingIterator<InternalKey, Slice> iterator = tableCache.newIterator(fileMetaData);

            // seek to the key
            iterator.seek(key.getInternalKey());

            if (iterator.hasNext()) {
                // parse the key in the block
                Entry<InternalKey, Slice> entry = iterator.next();
                InternalKey internalKey = entry.getKey();
                Preconditions.checkState(internalKey != null, "Corrupt key for %s", key.getUserKey().toString(UTF_8));

                // if this is a value key (not a delete) and the keys match, return the value
                if (key.getUserKey().equals(internalKey.getUserKey())) {
                    if (internalKey.getValueType() == ValueType.DELETION) {
                        return LookupResult.deleted(key);
                    }
                    else if (internalKey.getValueType() == VALUE) {
                        return LookupResult.ok(key, entry.getValue());
                    }
                }
            }

            if (readStats.getSeekFile() == null) {
                // We have had more than one seek for this read.  Charge the first file.
                readStats.setSeekFile(fileMetaData);
                readStats.setSeekFileLevel(levelNumber);
            }
        }

        return null;
    }

    private static <T> int ceilingEntryIndex(List<T> list, T key, Comparator<T> comparator)
    {
        int insertionPoint = Collections.binarySearch(list, key, comparator);
        if (insertionPoint < 0) {
            insertionPoint = -(insertionPoint + 1);
        }
        return insertionPoint;
    }

    public boolean someFileOverlapsRange(Slice smallestUserKey, Slice largestUserKey)
    {
        InternalKey smallestInternalKey = new InternalKey(smallestUserKey, MAX_SEQUENCE_NUMBER, VALUE);
        int index = findFile(smallestInternalKey);

        UserComparator userComparator = internalKeyComparator.getUserComparator();
        return ((index < files.size()) &&
                userComparator.compare(largestUserKey, files.get(index).getSmallest().getUserKey()) >= 0);
    }

    private int findFile(InternalKey targetKey)
    {
        if (files.size() == 0) {
            return files.size();
        }

        // todo replace with Collections.binarySearch
        int left = 0;
        int right = files.size() - 1;

        // binary search restart positions to find the restart position immediately before the targetKey
        while (left < right) {
            int mid = (left + right) / 2;

            if (internalKeyComparator.compare(files.get(mid).getLargest(), targetKey) < 0) {
                // Key at "mid.largest" is < "target".  Therefore all
                // files at or before "mid" are uninteresting.
                left = mid + 1;
            }
            else {
                // Key at "mid.largest" is >= "target".  Therefore all files
                // after "mid" are uninteresting.
                right = mid;
            }
        }
        return right;
    }

    public void addFile(FileMetaData fileMetaData)
    {
        // todo remove mutation
        files.add(fileMetaData);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("Level");
        sb.append("{levelNumber=").append(levelNumber);
        sb.append(", files=").append(files);
        sb.append('}');
        return sb.toString();
    }

    public static class FileMetaDataSeekingIterator implements SeekingIterator<InternalKey, FileMetaData>
    {
        private final List<FileMetaData> files;
        private final InternalKeyComparator comparator;
        private int index;

        public FileMetaDataSeekingIterator(List<FileMetaData> files, InternalKeyComparator comparator)
        {
            Preconditions.checkNotNull(files, "files is null");
            Preconditions.checkNotNull(comparator, "comparator is null");

            this.files = ImmutableList.copyOf(files);
            this.comparator = comparator;
        }

        @Override
        public boolean hasNext()
        {
            return index < files.size();
        }

        @Override
        public void seekToFirst()
        {
            index = 0;
        }

        @Override
        public void seek(InternalKey targetKey)
        {
            if (files.size() == 0) {
                return;
            }

            // todo replace with Collections.binarySearch
            int left = 0;
            int right = files.size() - 1;

            // binary search restart positions to find the restart position immediately before the targetKey
            while (left < right) {
                int mid = (left + right) / 2;

                if (comparator.compare(files.get(mid).getLargest(), targetKey) < 0) {
                    // Key at "mid.largest" is < "target".  Therefore all
                    // files at or before "mid" are uninteresting.
                    left = mid + 1;
                }
                else {
                    // Key at "mid.largest" is >= "target".  Therefore all files
                    // after "mid" are uninteresting.
                    right = mid;
                }
            }
            index = right;

            // if the index is now pointing to the last block in the file, check if the largest key
            // in the block is than the the target key.  If so, we need to seek beyond the end of this file
            if (index == files.size() - 1 && comparator.compare(files.get(index).getLargest(), targetKey) < 0) {
                index++;
            }
        }

        @Override
        public Entry<InternalKey, FileMetaData> peek()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            FileMetaData fileMetaData = files.get(index);
            return Maps.immutableEntry(fileMetaData.getLargest(), fileMetaData);
        }

        @Override
        public Entry<InternalKey, FileMetaData> next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            FileMetaData fileMetaData = files.get(index);
            index++;
            return Maps.immutableEntry(fileMetaData.getLargest(), fileMetaData);
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            final StringBuilder sb = new StringBuilder();
            sb.append("FileMetaDataSeekingIterator");
            sb.append("{index=").append(index);
            sb.append(", files=").append(files);
            sb.append('}');
            return sb.toString();
        }
    }

}
