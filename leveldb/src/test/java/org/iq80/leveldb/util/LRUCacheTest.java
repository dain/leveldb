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

import com.google.common.cache.CacheLoader;
import com.google.common.cache.Weigher;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.concurrent.ExecutionException;

import static org.testng.Assert.assertEquals;

/**
 * @author Honore Vasconcelos
 */
public class LRUCacheTest
{
    @Test
    public void testMultipleClientWithSameKey() throws Exception
    {
        final LRUCache<Integer, Integer> cache = new LRUCache<>(2 * 5, new CountWeigher());
        final CacheWithStatistics[] caches = CacheWithStatistics.withStatistics(cache, 2);

        for (int x = 0; x < 3; ++x) {
            for (int i = 0; i < caches.length; ++i) {
                for (int j = 0; j < 5; ++j) {
                    assertEquals(((int) caches[i].load(j)), j * (i + 1) * 3);
                }
            }
            //only first run should load data into cache, as such, only 5 load should be executed instead of 30
            for (CacheWithStatistics cache1 : caches) {
                assertEquals(cache1.count, 5);
            }
        }
    }

    @Test
    public void testLimitIsRespected() throws Exception
    {
        // size is respected by guava but we could have some type of bug :)
        final LRUCache<Integer, Integer> cache = new LRUCache<>(2, new CountWeigher());
        final CacheWithStatistics[] caches = CacheWithStatistics.withStatistics(cache, 2);
        caches[0].load(0);
        caches[0].load(1);
        caches[0].load(2);
        caches[0].load(1);
        caches[0].load(0);

        assertEquals(caches[0].count, 4);
        assertEquals(caches[1].count, 0);

        caches[1].load(0);
        caches[0].load(0);
        assertEquals(caches[0].count, 4);
        assertEquals(caches[1].count, 1);

        caches[0].load(2);
        caches[1].load(1);
        assertEquals(caches[0].count, 5);
        assertEquals(caches[1].count, 2);
    }

    private static class CacheWithStatistics implements LRUCache.LRUSubCache<Integer, Integer>
    {
        private final LRUCache.LRUSubCache<Integer, Integer> cache;
        private int count;

        private CacheWithStatistics(LRUCache<Integer, Integer> cache, final int i)
        {
            this.cache = cache.subCache(new CacheLoader<Integer, Integer>()
            {
                @Override
                public Integer load(@Nonnull Integer key)
                {
                    count++;
                    return key * (i + 1) * 3;
                }
            });
        }

        static CacheWithStatistics[] withStatistics(LRUCache<Integer, Integer> cache, int clients)
        {
            final CacheWithStatistics[] caches = new CacheWithStatistics[clients];
            for (int i = 0; i < clients; ++i) {
                caches[i] = new CacheWithStatistics(cache, i);
            }
            return caches;
        }

        @Override
        public Integer load(@Nonnull Integer key) throws ExecutionException
        {
            return cache.load(key);
        }
    }

    private static class CountWeigher implements Weigher<Integer, Integer>
    {
        @Override
        public int weigh(Integer key, Integer value)
        {
            return -31; //hack to simplify unit test
        }
    }
}
