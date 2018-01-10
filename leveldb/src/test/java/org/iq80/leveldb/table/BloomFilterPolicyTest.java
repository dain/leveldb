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
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Honore Vasconcelos
 */
public class BloomFilterPolicyTest
{
    public static final int BLOOM_BITS = 10;
    private byte[] filter = new byte[0];
    private List<byte[]> keys = new ArrayList<>();
    private BloomFilterPolicy policy = new BloomFilterPolicy(BLOOM_BITS);

    @Test
    public void emptyBloom() throws Exception
    {
        Assert.assertTrue(!matches("hello"));
        Assert.assertTrue(!matches("world"));
    }

    @Test
    public void smallBloom() throws Exception
    {
        add("hello");
        add("world");
        Assert.assertTrue(matches("hello"), "Key should be found");
        Assert.assertTrue(matches("world"), "Key should be sound");
        Assert.assertTrue(!matches("x"));
        Assert.assertTrue(!matches("foo"));
    }

    @Test
    public void testVariableLength() throws Exception
    {
        // Count number of filters that significantly exceed the false positive rate
        int mediocreFilters = 0;
        int goodFilters = 0;

        for (int length = 1; length <= 10000; length = nextLength(length)) {
            reset();
            for (int i = 0; i < length; i++) {
                keys.add(intToBytes(i));
            }
            build();

            Assert.assertTrue(filter.length <= (length * BLOOM_BITS / 8) + 40);

            // All added keys must match
            for (int i = 0; i < length; i++) {
                Assert.assertTrue(matches(intToBytes(i)));
            }

            // Check false positive rate
            double rate = falsePositiveRate();
            System.err.print(String.format("False positives: %5.2f%% @ length = %6d ; bytes = %6d\n",
                    rate * 100.0, length, filter.length));

            Assert.assertTrue(rate <= 0.02);   // Must not be over 2%
            if (rate > 0.0125) {
                mediocreFilters++;  // Allowed, but not too often
            }
            else {
                goodFilters++;
            }
        }
        System.err.print(String.format("Filters: %d good, %d mediocre\n",
                goodFilters, mediocreFilters));
        Assert.assertTrue(mediocreFilters <= goodFilters / 5);

    }

    private double falsePositiveRate()
    {
        int result = 0;
        for (int i = 0; i < 10000; i++) {
            if (matches(intToBytes(i + 1000000000))) {
                result++;
            }
        }
        return result / 10000.0;
    }

    private byte[] intToBytes(int value)
    {
        byte[] buffer = new byte[4];
        buffer[0] = (byte) (value);
        buffer[1] = (byte) (value >>> 8);
        buffer[2] = (byte) (value >>> 16);
        buffer[3] = (byte) (value >>> 24);
        return buffer;
    }

    private void reset()
    {
        keys.clear();
        filter = new byte[0];
    }

    private static int nextLength(int length)
    {
        if (length < 10) {
            length += 1;
        }
        else if (length < 100) {
            length += 10;
        }
        else if (length < 1000) {
            length += 100;
        }
        else {
            length += 1000;
        }
        return length;
    }

    private void add(String hello)
    {
        keys.add(getBytes(hello));
    }

    private boolean matches(String s)
    {
        return matches(getBytes(s));
    }

    private boolean matches(byte[] s)
    {
        if (!keys.isEmpty()) {
            build();
        }
        return policy.keyMayMatch(new Slice(s), new Slice(filter));
    }

    private byte[] getBytes(String s)
    {
        return s.getBytes(Charset.forName("ISO-8859-1"));
    }

    private void build()
    {
        List<Slice> keySlices = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
            keySlices.add(new Slice(keys.get(i)));
        }
        filter = policy.createFilter(keySlices);
        keys.clear();
    }
}
