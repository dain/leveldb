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

import org.iq80.leveldb.util.Slice;
import org.testng.annotations.Test;

import static org.iq80.leveldb.util.TestUtils.asciiToSlice;
import static org.testng.Assert.assertEquals;

public class VersionEditTest
{
    @Test
    public void testEncodeDecode() throws Exception
    {
        long kBig = 1L << 50;

        VersionEdit edit = new VersionEdit();
        for (int i = 0; i < 4; i++) {
            testEncodeDecode(edit);
            edit.addFile(3, kBig + 300 + i, kBig + 400 + i,
                    new InternalKey(asciiToSlice("foo"), kBig + 500 + i, ValueType.VALUE),
                    new InternalKey(asciiToSlice("zoo"), kBig + 600 + i, ValueType.DELETION));
            edit.deleteFile(4, kBig + 700 + i);
            edit.setCompactPointer(i, new InternalKey(asciiToSlice("x"), kBig + 900 + i, ValueType.VALUE));
        }

        edit.setComparatorName("foo");
        edit.setLogNumber(kBig + 100);
        edit.setNextFileNumber(kBig + 200);
        edit.setLastSequenceNumber(kBig + 1000);
        testEncodeDecode(edit);
    }

    void testEncodeDecode(VersionEdit edit)
    {
        Slice encoded = edit.encode();
        VersionEdit parsed = new VersionEdit(encoded);
        Slice encoded2 = parsed.encode();
        assertEquals(encoded, encoded2);
    }
}
