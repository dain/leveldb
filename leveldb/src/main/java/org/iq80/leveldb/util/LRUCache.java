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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Single LRU cache that can be used by multiple clients, each client can use identical key without interfering with
 * other clients.
 * <p>
 * To obtain a new cache client instance simply use {@link LRUCache#subCache(CacheLoader)}.
 * <p>
 * Try to reduce number of objects created by having only one loader per client.
 *
 * @author Honore Vasconcelos
 */
public final class LRUCache<K, V>
{
    private final AtomicLong idGenerator = new AtomicLong();
    private final LoadingCache<CacheKey<K, V>, V> cache;
    private final CacheLoader<CacheKey<K, V>, V> valueLoader = new CacheLoader<CacheKey<K, V>, V>()
    {
        @Override
        public V load(CacheKey<K, V> key) throws Exception
        {
            return key.loader.load(key.key);
        }
    };

    public LRUCache(int capacity, final Weigher<K, V> weigher)
    {
        this.cache = CacheBuilder.<CacheKey<K, V>, V>newBuilder()
                .maximumWeight(capacity)
                .weigher(new CacheKeyWeigher<>(weigher))
                .concurrencyLevel(1 << 4)
                .build(valueLoader);
    }

    public LRUSubCache<K, V> subCache(final CacheLoader<K, V> loader)
    {
        return new LRUSubCache<K, V>()
        {
            private final long id = idGenerator.incrementAndGet();

            @Override
            public V load(K k) throws ExecutionException
            {
                return cache.get(new CacheKey<>(id, k, loader));
            }
        };
    }

    public interface LRUSubCache<K, V>
    {
        V load(K key) throws ExecutionException;
    }

    public static final class CacheKey<K, M>
    {
        private final long id;
        private final K key;
        //not part of the key, but avoid creating objects for each key search
        private final CacheLoader<K, M> loader;

        CacheKey(final long id, K key, CacheLoader<K, M> loader)
        {
            this.id = id;
            this.key = key;
            this.loader = loader;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            CacheKey<?, ?> cacheKey = (CacheKey<?, ?>) o;

            return id == cacheKey.id && key.equals(cacheKey.key);
        }

        @Override
        public int hashCode()
        {
            int result = (int) (id ^ (id >>> 32));
            result = 31 * result + key.hashCode();
            return result;
        }
    }

    private static final class CacheKeyWeigher<K, V> implements Weigher<CacheKey<K, V>, V>
    {
        private final Weigher<K, V> weigher;

        CacheKeyWeigher(Weigher<K, V> weigher)
        {
            this.weigher = weigher;
        }

        @Override
        public int weigh(CacheKey<K, V> key, V value)
        {
            return 32 + weigher.weigh(key.key, value);
        }
    }
}
