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

import org.iq80.leveldb.util.Slice;

/**
 * @author Honore Vasconcelos
 */
final class FilterBlockReader
{
    private final byte baseLg;
    private final int num;
    private final Slice contents;
    private final int offset;
    private final FilterPolicy filterPolicy;

    FilterBlockReader(FilterPolicy filterPolicy, Slice contents)
    {
        this.filterPolicy = filterPolicy;
        final int n = contents.length();
        final int lgAndOffset = 5;
        if (n < lgAndOffset) { //byte + int
            this.baseLg = 0;
            this.contents = null;
            this.num = 0;
            this.offset = 0;
            return;
        }
        baseLg = contents.getByte(n - 1);
        offset = contents.getInt(n - lgAndOffset);
        if (offset > n - lgAndOffset) {
            this.num = 0;
            this.contents = null;
            return;
        }
        num = (n - lgAndOffset - offset) / 4;
        this.contents = contents;
    }

    public boolean keyMayMatch(long offset1, Slice key)
    {
        final int index = (int) (offset1 >> baseLg);
        if (index < num) {
            final int start = contents.getInt(this.offset + index * 4);
            final int limit = contents.getInt(this.offset + index * 4 + 4);
            if (start <= limit && limit <= offset) {
                Slice filter = contents.slice(start, limit - start);
                return filterPolicy.keyMayMatch(key, filter);
            }
            else if (start == limit) {
                // Empty filters do not match any keys
                return false;
            }
        }
        return true;  // Errors are treated as potential matches
    }
}
