package org.iq80.leveldb.impl;

import java.io.File;
import java.io.IOException;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.util.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DBTest {
    private File databaseDir;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        databaseDir = FileUtils.createTempDir("leveldb");
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        FileUtils.deleteRecursively(databaseDir);
    }

    @Test
    void test1() throws IOException {
        DB db = Iq80DBFactory.factory.open(databaseDir, new Options());

        // Add an element
        byte[] ONE = new byte[] { 1 };
        Assert.assertNull(db.get(ONE));
        WriteOptions writeOptions = new WriteOptions();
        writeOptions.sync(true);
        db.put(ONE, ONE, writeOptions);
        Assert.assertNotNull(db.get(ONE));

        // Delete it
        db.delete(ONE, writeOptions);

        // Test that it's gone with get
        Assert.assertNull(db.get(ONE));

        // Test that it's gone with iterator
        DBIterator iter = db.iterator();
        iter.seekToFirst();
        long size = 0;
        while (iter.hasNext()) {
            iter.next();
            size++;
        }
        Assert.assertEquals(size, 0);
    }
}
