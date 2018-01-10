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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.iq80.leveldb.XFilterPolicy;
import org.iq80.leveldb.util.Slice;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Filter policy wrapper that converts from internal keys to user keys
 * <p>
 * <p>
 *
 * @author Honore Vasconcelos
 */
final class InternalFilterPolicy implements org.iq80.leveldb.table.FilterPolicy
{
    private static final Function<Slice, Slice> EXTRACT_USER_KEY = InternalFilterPolicy::extractUserKey;
    private org.iq80.leveldb.table.FilterPolicy userPolicy;

    private InternalFilterPolicy(org.iq80.leveldb.table.FilterPolicy userPolicy)
    {
        this.userPolicy = userPolicy;
    }

    static InternalFilterPolicy convert(XFilterPolicy policy)
    {
        checkArgument(policy == null || policy instanceof org.iq80.leveldb.table.FilterPolicy, "Filter policy must implement Java interface FilterPolicy");
        if (policy instanceof InternalFilterPolicy) {
            return (InternalFilterPolicy) policy;
        }
        return policy == null ? null : new InternalFilterPolicy((org.iq80.leveldb.table.FilterPolicy) policy);
    }

    @Override
    public String name()
    {
        return userPolicy.name();
    }

    @Override
    public byte[] createFilter(final List<Slice> keys)
    {
        //instead of copying all the keys to a shorter form, make it lazy
        return userPolicy.createFilter(Lists.transform(keys, EXTRACT_USER_KEY));
    }

    @Override
    public boolean keyMayMatch(Slice key, Slice filter)
    {
        return userPolicy.keyMayMatch(extractUserKey(key), filter);
    }

    private static Slice extractUserKey(Slice key)
    {
        checkArgument(key.length() >= 8);
        return key.slice(0, key.length() - 8);
    }
}
