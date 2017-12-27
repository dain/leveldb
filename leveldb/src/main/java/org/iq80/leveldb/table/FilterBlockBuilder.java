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

package org.iq80.leveldb.table;

import org.iq80.leveldb.util.DynamicSliceOutput;
import org.iq80.leveldb.util.IntVector;
import org.iq80.leveldb.util.Slice;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * The filter block stores a sequence of filters, where filter i contains
 * the output of FilterPolicy::CreateFilter() on all keys that are stored
 * in a block whose file offset falls within the range
 * <p>
 * [ i*base ... (i+1)*base-1 ]
 * <p>
 * Currently, "base" is 2KB.  So for example, if blocks X and Y start in
 * the range [ 0KB .. 2KB-1 ], all of the keys in X and Y will be
 * converted to a filter by calling FilterPolicy::CreateFilter(), and the
 * resulting filter will be stored as the first filter in the filter
 * block.
 * <p>
 * The filter block is formatted as follows:
 * <p>
 * [filter 0]
 * [filter 1]
 * [filter 2]
 * ...
 * [filter N-1]
 * <p>
 * [offset of filter 0]                  : 4 bytes
 * [offset of filter 1]                  : 4 bytes
 * [offset of filter 2]                  : 4 bytes
 * ...
 * [offset of filter N-1]                : 4 bytes
 * <p>
 * [offset of beginning of offset array] : 4 bytes
 * lg(base)                              : 1 byte
 * <p>
 * <p>
 *
 * @author Honore Vasconcelos
 */
public class FilterBlockBuilder
{
    // Generate new filter every 2KB of data
    private static final byte FILTER_BASE_LG = 11;
    private static final int FILTER_BASE = 1 << FILTER_BASE_LG;

    private final List<Slice> keys = new ArrayList<>();
    private final DynamicSliceOutput result = new DynamicSliceOutput(32);
    private final IntVector filterOffsets = new IntVector(32);
    private final FilterPolicy policy;

    public FilterBlockBuilder(FilterPolicy policy)
    {
        this.policy = policy;
    }

    public void addKey(Slice key)
    {
        keys.add(key);
    }

    public void startBlock(long blockOffset)
    {
        long filterIndex = blockOffset / FILTER_BASE;
        checkArgument(filterIndex >= filterOffsets.size());
        while (filterIndex > filterOffsets.size()) {
            generateFilter();
        }
    }

    private void generateFilter()
    {
        final int numberOfKeys = keys.size();
        if (numberOfKeys == 0) {
            //Fast path if there are no keys for this filter
            filterOffsets.add(result.size());
            return;
        }
        filterOffsets.add(result.size());
        final byte[] filter = policy.createFilter(keys);
        result.writeBytes(filter);
        keys.clear();
    }

    public Slice finish()
    {
        if (!keys.isEmpty()) {
            generateFilter();
        }
        final int arrayOffset = result.size();
        filterOffsets.write(result);
        result.writeInt(arrayOffset); //4 bytes
        result.write(FILTER_BASE_LG); //1 byte
        final Slice slice = result.slice();
        return slice;
    }

    public String name()
    {
        return policy.name();
    }
}
