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

package org.iq80.leveldb.impl;

import org.iq80.leveldb.table.KeyValueFunction;
import org.iq80.leveldb.util.Slice;

import static com.google.common.base.Preconditions.checkState;
import static org.iq80.leveldb.impl.ValueType.VALUE;

/**
 * @author Honore Vasconcelos
 */
public class KeyMatchingLookup implements KeyValueFunction<LookupResult>
{
    private LookupKey key;

    KeyMatchingLookup(LookupKey key)
    {
        this.key = key;
    }

    @Override
    public LookupResult apply(Slice internalKey1, Slice value)
    {
        // parse the key in the block
        checkState(internalKey1 != null, "Corrupt key for %s", key);

        final InternalKey internalKey = new InternalKey(internalKey1);

        // if this is a value key (not a delete) and the keys match, return the value
        if (key.getUserKey().equals(internalKey.getUserKey())) {
            if (internalKey.getValueType() == ValueType.DELETION) {
                return LookupResult.deleted(key);
            }
            else if (internalKey.getValueType() == VALUE) {
                return LookupResult.ok(key, value);
            }
        }
        return null;
    }
}
