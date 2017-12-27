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

import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.util.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.iq80.leveldb.impl.Iq80DBFactory.asString;
import static org.iq80.leveldb.impl.Iq80DBFactory.bytes;
import static org.testng.Assert.assertTrue;

/**
 * Test the implemenation via the org.iq80.leveldb API.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ApiTest
{
    private final File databaseDir = FileUtils.createTempDir("leveldb");

    public void assertEquals(byte[] arg1, byte[] arg2)
    {
        assertTrue(Arrays.equals(arg1, arg2), asString(arg1) + " != " + asString(arg2));
    }

    private final DBFactory factory = Iq80DBFactory.factory;

    File getTestDirectory(String name)
            throws IOException
    {
        File rc = new File(databaseDir, name);
        factory.destroy(rc, new Options().createIfMissing(true));
        rc.mkdirs();
        return rc;
    }

    @DataProvider(name = "options")
    public Object[][] optionsProvider()
    {
        return new Object[][] {
                {"mm/mm", new Options().createIfMissing(true).allowMmapWrites(true).allowMmapReads(true).compressionType(CompressionType.NONE)},
                {"mm/raf", new Options().createIfMissing(true).allowMmapWrites(true).allowMmapReads(false).compressionType(CompressionType.NONE)},
                {"raf/mm", new Options().createIfMissing(true).allowMmapWrites(false).allowMmapReads(true).compressionType(CompressionType.NONE)},
                {"raf/raf", new Options().createIfMissing(true).allowMmapWrites(false).allowMmapReads(false).compressionType(CompressionType.NONE)},
        };
    }

    @Test(invocationCount = 2, dataProvider = "options")
    public void testCompaction(String op, Options options)
            throws IOException, DBException
    {
        File path = getTestDirectory("testCompaction");
        DB db = factory.open(path, options);

        System.out.println("Adding");
        for (int i = 0; i < 1000 * 1000; i++) {
            if (i % 100000 == 0) {
                System.out.println("  at: " + i);
            }
            db.put(bytes("key" + i), bytes("value" + i));
        }

        db.close();
        db = factory.open(path, options);

        System.out.println("Deleting");
        for (int i = 0; i < 1000 * 1000; i++) {
            if (i % 100000 == 0) {
                System.out.println("  at: " + i);
            }
            db.delete(bytes("key" + i));
        }

        db.close();
        db = factory.open(path, options);

        System.out.println("Adding");
        for (int i = 0; i < 1000 * 1000; i++) {
            if (i % 100000 == 0) {
                System.out.println("  at: " + i);
            }
            db.put(bytes("key" + i), bytes("value" + i));
        }

        db.close();
    }

    @AfterMethod
    public void tearDown()
    {
        FileUtils.deleteRecursively(databaseDir);
    }
}
