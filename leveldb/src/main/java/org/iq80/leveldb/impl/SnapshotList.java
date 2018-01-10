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

import org.iq80.leveldb.Snapshot;

import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Snapshots are kept in a doubly-linked list in the DB.
 * Each Snapshot corresponds to a particular sequence number.
 */
final class SnapshotList
{
    private final ReentrantLock mutex;
    private final SnapshotNode list;

    /**
     * Snapshot list where all operation are protected by {@ode mutex}.
     * All {@code mutex} acquisition mut be done externally to ensure sequence order.
     *
     * @param mutex protect concurrent read/write to this list
     */
    public SnapshotList(ReentrantLock mutex)
    {
        this.mutex = mutex;
        this.list = new SnapshotNode(0);
        this.list.next = this.list;
        this.list.prev = this.list;
    }

    /**
     * Track a new snapshot for {@code sequence}.
     *
     * @param sequence most actual version sequence available
     * @return new a new tracked snapshot for {@code sequence}
     * @throws IllegalStateException if mutex is not held by current thread
     */
    public Snapshot newSnapshot(long sequence)
    {
        checkState(mutex.isHeldByCurrentThread());
        SnapshotNode s = new SnapshotNode(sequence);
        s.next = this.list;
        s.prev = list.prev;
        s.prev.next = s;
        s.next.prev = s;
        return s;
    }

    /**
     * Return {@code true} if list is empty
     *
     * @return Return {@code true} if list is empty
     * @throws IllegalStateException if mutex is not held by current thread
     */
    public boolean isEmpty()
    {
        checkState(mutex.isHeldByCurrentThread());
        return list.next == list;
    }

    /**
     * Return oldest sequence number of this list
     *
     * @return oldest sequence number
     * @throws IllegalStateException if mutex is not held by current thread or list is empty
     */
    public long getOldest()
    {
        checkState(mutex.isHeldByCurrentThread());
        checkState(!isEmpty());
        return list.next.number;
    }

    /**
     * Return sequence corresponding to given snapshot.
     *
     * @param snapshot snapshot to read from
     * @return Return sequence corresponding to given snapshot.
     * @throws IllegalArgumentException if snapshot concrete type does not come from current list
     * @throws IllegalStateException if mutex is not held by current thread
     */
    public long getSequenceFrom(Snapshot snapshot)
    {
        checkArgument(snapshot instanceof SnapshotNode);
        checkState(mutex.isHeldByCurrentThread());
        return ((SnapshotNode) snapshot).number;
    }

    private final class SnapshotNode implements Snapshot
    {
        private final long number;
        private SnapshotNode next;
        private SnapshotNode prev;

        private SnapshotNode(long number)
        {
            this.number = number;
        }

        @Override
        public void close()
        {
            mutex.lock();
            try {
                this.prev.next = this.next;
                this.next.prev = this.prev;
            }
            finally {
                mutex.unlock();
            }
        }
    }
}
