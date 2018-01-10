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

import org.iq80.leveldb.util.LRUCache;
import org.iq80.leveldb.util.MMRandomInputFile;
import org.iq80.leveldb.util.Slice;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

public class MMRandomInputFileTableTest
        extends TableTest
{
    @Override
    protected Table createTable(File file, Comparator<Slice> comparator, boolean verifyChecksums, FilterPolicy filterPolicy)
            throws IOException
    {
        return new Table(MMRandomInputFile.open(file), comparator, verifyChecksums, new LRUCache<>(8 << 20, new BlockHandleSliceWeigher()), filterPolicy);
    }
}
