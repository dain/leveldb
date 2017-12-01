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

import java.util.List;

/**
 * A database can be configured with a custom FilterPolicy object.
 * This object is responsible for creating a small filter from a set
 * of keys.  These filters are stored in leveldb and are consulted
 * automatically by leveldb to decide whether or not to read some
 * information from disk. In many cases, a filter can cut down the
 * number of disk seeks form a handful to a single disk seek per
 * DB::Get() call.
 * <p>
 * Most people will want to use the builtin bloom filter support (see
 * NewBloomFilterPolicy() below).
 *
 * @author Honore Vasconcelos
 */
public interface FilterPolicy extends org.iq80.leveldb.XFilterPolicy
{
    String name();

    /**
     * Append a filter that summarizes keys[0,n-1] to *dst.
     *
     * @param keys keys[0,n-1] contains a list of keys (potentially with duplicates)
     *             that are ordered according to the user supplied comparator.
     */
    byte[] createFilter(List<Slice> keys);

    /**
     * "filter" contains the data appended by a preceding call to
     * CreateFilter() on this class.  This method must return true if
     * the key was in the list of keys passed to CreateFilter().
     * This method may return true or false if the key was not on the
     * list, but it should aim to return false with a high probability.
     */
    boolean keyMayMatch(Slice key, Slice filter);
}
