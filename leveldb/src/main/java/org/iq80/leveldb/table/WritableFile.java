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
package org.iq80.leveldb.table;

import org.iq80.leveldb.util.Slice;

import java.io.Closeable;
import java.io.IOException;

/**
 * A file abstraction for sequential writing.  The implementation
 * must provide buffering since callers may append small fragments
 * at a time to the file.
 *
 * @author Honore Vasconcelos
 */
public interface WritableFile extends Closeable
{
    /**
     * Append {@code data} to current file position.
     * @param data data to append
     * @throws IOException
     */
    void append(Slice data) throws IOException;

    /**
     * Force sync bytes to filesystem.
     * @throws IOException
     */
    void force() throws IOException;
}
