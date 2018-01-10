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

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;

import static org.testng.AssertJUnit.assertEquals;

public class SequentialFileImplTest
{
    File file;

    @BeforeMethod
    public void setUp() throws Exception
    {
        file = File.createTempFile("test", ".log");
    }

    @Test
    public void testCheckReadBounds() throws Exception
    {
        try (FileOutputStream f = new FileOutputStream(file)) {
            for (int i = 0; i < 200; ++i) {
                f.write(i);
            }
        }
        try (SequentialFile open = SequentialFileImpl.open(file)) {
            DynamicSliceOutput destination = new DynamicSliceOutput(10);
            assertEquals(10, open.read(10, destination));
            Slice slice = destination.slice();
            assertEquals(new Slice(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}), slice);
            byte[] bytes = new byte[190];
            for (int i = 10, k = 0; i < 200; ++i, k++) {
                bytes[k] = (byte) i;
            }
            DynamicSliceOutput destination1 = new DynamicSliceOutput(10);
            assertEquals(190, open.read(200, destination1));
            Slice slice1 = destination1.slice();
            assertEquals(new Slice(bytes), slice1);
            assertEquals(-1, open.read(10, new DynamicSliceOutput(10))); //EOF
            assertEquals(0, open.read(0, new DynamicSliceOutput(10))); //EOF
        }
    }

    @AfterMethod
    public void tearDown()
    {
        file.delete();
    }
}
