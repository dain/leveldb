/**
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
package org.iq80.leveldb.api;

public class ReadOptions
{
    private boolean verifyChecksums = false;
    private boolean fillCache = true;
    private Snapshot snapshot;

    public Snapshot getSnapshot()
    {
        return snapshot;
    }

    public ReadOptions setSnapshot(Snapshot snapshot)
    {
        this.snapshot = snapshot;
        return this;
    }

    public boolean isFillCache() {
        return fillCache;
    }

    public ReadOptions setFillCache(boolean fillCache) {
        this.fillCache = fillCache;
        return this;
    }

    public boolean isVerifyChecksums() {
        return verifyChecksums;
    }

    public ReadOptions setVerifyChecksums(boolean verifyChecksums) {
        this.verifyChecksums = verifyChecksums;
        return this;
    }
}
