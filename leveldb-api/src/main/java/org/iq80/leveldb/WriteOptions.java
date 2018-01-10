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
package org.iq80.leveldb;

public class WriteOptions
{
    private boolean sync;
    private boolean snapshot;

    /**
     * If true, the write will be flushed from the operating system
     * buffer cache (by calling WritableFile::Sync()) before the write
     * is considered complete.  If this flag is true, writes will be
     * slower.
     * <p>
     * If this flag is false, and the machine crashes, some recent
     * writes may be lost.  Note that if it is just the process that
     * crashes (i.e., the machine does not reboot), no writes will be
     * lost even if sync==false.
     * <p>
     * In other words, a DB write with sync==false has similar
     * crash semantics as the "write()" system call.  A DB write
     * with sync==true has similar crash semantics to a "write()"
     * system call followed by "fsync()".
     * <p>
     *     In java Implementation if process crash
     * Default: false
     **/
    public boolean sync()
    {
        return sync;
    }

    public WriteOptions sync(boolean sync)
    {
        this.sync = sync;
        return this;
    }

    public boolean snapshot()
    {
        return snapshot;
    }

    public WriteOptions snapshot(boolean snapshot)
    {
        this.snapshot = snapshot;
        return this;
    }
}
