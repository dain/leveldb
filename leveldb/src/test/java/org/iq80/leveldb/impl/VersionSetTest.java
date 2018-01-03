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

import org.iq80.leveldb.Options;
import org.iq80.leveldb.table.BytewiseComparator;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.TestUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class VersionSetTest
{
    private List<FileMetaData> files = new ArrayList<>();

    @BeforeMethod
    public void setUp() throws Exception
    {
        files.clear();
    }

    void add(String smallest, String largest,
             long smallestSeq,
             long largestSeq)
    {
        files.add(new FileMetaData(files.size() + 1, 0, new InternalKey(TestUtils.asciiToSlice(smallest), smallestSeq, ValueType.VALUE), new InternalKey(TestUtils.asciiToSlice(largest), largestSeq, ValueType.VALUE)));
    }

    int find(String key)
    {
        InternalKey target = new InternalKey(TestUtils.asciiToSlice(key), 100, ValueType.VALUE);
        return newLevel().findFile(target);
    }

    private Level newLevel()
    {
        InternalKeyComparator internalKeyComparator = new InternalKeyComparator(new BytewiseComparator());
        return new Level(1, files, new TableCache(new File("xxxxxxxxxxx"), 0, new BytewiseComparator(), new Options()), internalKeyComparator);
    }

    boolean overlaps(String smallest, String largest)
    {
        Slice s = smallest != null ? TestUtils.asciiToSlice(smallest) : null;
        Slice l = largest != null ? TestUtils.asciiToSlice(largest) : null;
        return newLevel().someFileOverlapsRange(true, s, l);
    }

    @Test
    public void testEmpty() throws Exception
    {
        assertEquals(find("foo"), 0);
        assertFalse(overlaps("z", "a"));
        assertFalse(overlaps("z", null));
        assertFalse(overlaps("a", null));
        assertFalse(overlaps(null, null));
    }

    @Test
    public void testSingle() throws Exception
    {
        add("p", "q", 100, 100);
        assertEquals(find("a"), 0);
        assertEquals(find("p"), 0);
        assertEquals(find("pl"), 0);
        assertEquals(find("q"), 0);
        assertEquals(find("ql"), 1);
        assertEquals(find("z"), 1);

        assertTrue(!overlaps("a", "b"));
        assertTrue(!overlaps("z1", "z2"));
        assertTrue(overlaps("a", "p"));
        assertTrue(overlaps("a", "q"));
        assertTrue(overlaps("a", "z"));
        assertTrue(overlaps("p", "p1"));
        assertTrue(overlaps("p", "q"));
        assertTrue(overlaps("p", "z"));
        assertTrue(overlaps("p1", "p2"));
        assertTrue(overlaps("p1", "z"));
        assertTrue(overlaps("q", "q"));
        assertTrue(overlaps("q", "q1"));

        assertTrue(!overlaps(null, "j"));
        assertTrue(!overlaps("r", null));
        assertTrue(overlaps(null, "p"));
        assertTrue(overlaps(null, "p1"));
        assertTrue(overlaps("q", null));
        assertTrue(overlaps(null, null));
    }

    @Test
    public void testMultiple() throws Exception
    {
        add("150", "200", 100, 100);
        add("200", "250", 100, 100);
        add("300", "350", 100, 100);
        add("400", "450", 100, 100);
        assertEquals(0, find("100"));
        assertEquals(0, find("150"));
        assertEquals(0, find("151"));
        assertEquals(0, find("199"));
        assertEquals(0, find("200"));
        assertEquals(1, find("201"));
        assertEquals(1, find("249"));
        assertEquals(1, find("250"));
        assertEquals(2, find("251"));
        assertEquals(2, find("299"));
        assertEquals(2, find("300"));
        assertEquals(2, find("349"));
        assertEquals(2, find("350"));
        assertEquals(3, find("351"));
        assertEquals(3, find("400"));
        assertEquals(3, find("450"));
        assertEquals(4, find("451"));

        assertTrue(!overlaps("100", "149"));
        assertTrue(!overlaps("251", "299"));
        assertTrue(!overlaps("451", "500"));
        assertTrue(!overlaps("351", "399"));

        assertTrue(overlaps("100", "150"));
        assertTrue(overlaps("100", "200"));
        assertTrue(overlaps("100", "300"));
        assertTrue(overlaps("100", "400"));
        assertTrue(overlaps("100", "500"));
        assertTrue(overlaps("375", "400"));
        assertTrue(overlaps("450", "450"));
        assertTrue(overlaps("450", "500"));
    }

    @Test
    public void testMultipleNullBoundaries() throws Exception
    {
        add("150", "200", 100, 100);
        add("200", "250", 100, 100);
        add("300", "350", 100, 100);
        add("400", "450", 100, 100);
        assertTrue(!overlaps(null, "149"));
        assertTrue(!overlaps("451", null));
        assertTrue(overlaps(null, null));
        assertTrue(overlaps(null, "150"));
        assertTrue(overlaps(null, "199"));
        assertTrue(overlaps(null, "200"));
        assertTrue(overlaps(null, "201"));
        assertTrue(overlaps(null, "400"));
        assertTrue(overlaps(null, "800"));
        assertTrue(overlaps("100", null));
        assertTrue(overlaps("200", null));
        assertTrue(overlaps("449", null));
        assertTrue(overlaps("450", null));
    }

    @Test
    public void testOverlapSequenceChecks() throws Exception
    {
        add("200", "200", 5000, 3000);
        assertTrue(!overlaps("199", "199"));
        assertTrue(!overlaps("201", "300"));
        assertTrue(overlaps("200", "200"));
        assertTrue(overlaps("190", "200"));
        assertTrue(overlaps("200", "210"));
    }
}
