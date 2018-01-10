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

package org.iq80.leveldb.util;

/**
 * @author Honore Vasconcelos
 */
public final class Hash
{
    private Hash()
    {
    }

    public static int hash(byte[] data, int seed)
    {
        return hash(data, 0, data.length, seed);
    }

    /**
     * Partial array hash that start at offset and with len size.
     *
     * @param data   full data
     * @param offset data start offset
     * @param len    length of data
     * @param seed   hash seed
     * @return hash (sign has no meaning)
     */
    public static int hash(byte[] data, int offset, int len, int seed)
    {
        final int endIdx = len + offset;
        // Similar to murmur hash
        int m = 0xc6a4a793;
        int r = 24;

        int h = seed ^ (len * m);

        int idx = offset;
        // Pick up four bytes at a time
        for (; idx + 4 <= endIdx; idx += 4) {
            int w = byteToInt(data, idx);
            h += w;
            h *= m;
            h ^= (h >>> 16);
        }

        // Pick up remaining bytes
        final int remaining = endIdx - idx;
        switch (remaining) {
            case 3:
                h += (data[idx + 2] & 0xff) << 16;
                //FALLTHROUGH INTENDED: DO NOT PUT BREAK
            case 2:
                h += (data[idx + 1] & 0xff) << 8;
                //FALLTHROUGH INTENDED: DO NOT PUT BREAK
            case 1:
                h += data[idx] & 0xff;
                h *= m;
                h ^= (h >>> r);
                break;
        }
        return h;
    }

    private static int byteToInt(byte[] data, final int index)
    {
        return (data[index] & 0xff) |
                (data[index + 1] & 0xff) << 8 |
                (data[index + 2] & 0xff) << 16 |
                (data[index + 3] & 0xff) << 24;
    }
}
