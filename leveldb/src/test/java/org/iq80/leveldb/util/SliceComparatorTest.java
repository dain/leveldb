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

import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.iq80.leveldb.util.SliceComparator.SLICE_COMPARATOR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SliceComparatorTest
{
    @Test
    public void testSliceComparison()
    {
        assertTrue(SLICE_COMPARATOR.compare(
                Slices.copiedBuffer("beer/ipa", UTF_8),
                Slices.copiedBuffer("beer/ale", UTF_8))
                > 0);

        assertTrue(SLICE_COMPARATOR.compare(
                Slices.wrappedBuffer(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}),
                Slices.wrappedBuffer(new byte[] {(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00}))
                > 0);

        assertTrue(SLICE_COMPARATOR.compare(
                Slices.wrappedBuffer(new byte[] {(byte) 0xFF}),
                Slices.wrappedBuffer(new byte[] {(byte) 0x00}))
                > 0);

        assertAllEqual(Slices.copiedBuffer("abcdefghijklmnopqrstuvwxyz", UTF_8),
                Slices.copiedBuffer("abcdefghijklmnopqrstuvwxyz", UTF_8));
    }

    public static void assertAllEqual(Slice left, Slice right)
    {
        for (int i = 0; i < left.length(); i++) {
            assertEquals(SLICE_COMPARATOR.compare(left.slice(0, i), right.slice(0, i)), 0);
            assertEquals(SLICE_COMPARATOR.compare(right.slice(0, i), left.slice(0, i)), 0);
        }
        // differ in last byte only
        for (int i = 1; i < left.length(); i++) {
            Slice slice = right.slice(0, i);
            int lastReadableByte = slice.length() - 1;
            slice.setByte(lastReadableByte, slice.getByte(lastReadableByte) + 1);
            assertTrue(SLICE_COMPARATOR.compare(left.slice(0, i), slice) < 0);
            assertTrue(SLICE_COMPARATOR.compare(slice, left.slice(0, i)) > 0);
        }
    }
}
