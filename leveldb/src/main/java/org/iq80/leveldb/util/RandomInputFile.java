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
import java.nio.ByteBuffer;

/**
 * Read only data source for table data/blocks.
 *
 * @author Honore Vasconcelos
 */
public interface RandomInputFile extends Closeable
{
    /**
     * Source size
     */
    long size();

    /**
     * Read {@code length} bytes from source from {@code source} starting at {@code offset} position.
     * @param offset position for read start
     * @param length length of the bytes to read
     * @return read only view of the data.
     * @throws IOException on any exception will accessing source media
     */
    ByteBuffer read(long offset, int length) throws IOException;
}
