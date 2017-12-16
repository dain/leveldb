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

import java.io.Closeable;
import java.io.IOException;

public interface SequentialFile extends Closeable
{
    /**
     * Skips over and discards <code>n</code> bytes of data from the
     * input stream.
     *
     * @param n the number of bytes to be skipped.
     * @throws IOException if n is negative, if the stream does not
     *                     support seek, or if an I/O error occurs.
     */
    void skip(long n) throws IOException;

    /**
     * Read up to "atMost" bytes from the file.
     *
     * @param atMost      the maximum number of bytes to read.
     * @param destination data destination
     * @return the total number of bytes read into the destination, or
     * <code>-1</code> if there is no more data because the end of
     * the stream has been reached.
     * @throws IOException If the first byte cannot be read for any reason
     *                     other than end of file, or if the input stream has been closed, or if
     *                     some other I/O error occurs.
     */
    int read(int atMost, SliceOutput destination) throws IOException;
}
