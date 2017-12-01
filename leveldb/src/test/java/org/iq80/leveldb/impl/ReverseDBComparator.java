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

import com.google.common.primitives.UnsignedBytes;
import org.iq80.leveldb.DBComparator;

import java.util.Arrays;

public class ReverseDBComparator
        implements DBComparator
{
    @Override
    public String name()
    {
        return "test";
    }

    @Override
    public int compare(byte[] sliceA, byte[] sliceB)
    {
        // reverse order
        return -(UnsignedBytes.lexicographicalComparator().compare(sliceA, sliceB));
    }

    @Override
    public byte[] findShortestSeparator(byte[] start, byte[] limit)
    {
        // Find length of common prefix
        int sharedBytes = calculateSharedBytes(start, limit);

        // Do not shorten if one string is a prefix of the other
        if (sharedBytes < Math.min(start.length, limit.length)) {
            // if we can add one to the last shared byte without overflow and the two keys differ by more than
            // one increment at this location.
            int lastSharedByte = start[sharedBytes] & 0xff;
            if (lastSharedByte < 0xff && lastSharedByte + 1 < limit[sharedBytes]) {
                byte[] result = Arrays.copyOf(start, sharedBytes + 1);
                result[sharedBytes] = (byte) (lastSharedByte + 1);

                assert (compare(result, limit) < 0) : "start must be less than last limit";
                return result;
            }
        }
        return start;
    }

    @Override
    public byte[] findShortSuccessor(byte[] key)
    {
        // Find first character that can be incremented
        for (int i = 0; i < key.length; i++) {
            int b = key[i];
            if (b != 0xff) {
                byte[] result = Arrays.copyOf(key, i + 1);
                result[i] = (byte) (b + 1);
                return result;
            }
        }
        // key is a run of 0xffs.  Leave it alone.
        return key;
    }

    private int calculateSharedBytes(byte[] leftKey, byte[] rightKey)
    {
        int sharedKeyBytes = 0;

        if (leftKey != null && rightKey != null) {
            int minSharedKeyBytes = Math.min(leftKey.length, rightKey.length);
            while (sharedKeyBytes < minSharedKeyBytes && leftKey[sharedKeyBytes] == rightKey[sharedKeyBytes]) {
                sharedKeyBytes++;
            }
        }

        return sharedKeyBytes;
    }
}
