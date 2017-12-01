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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Honore Vasconcelos
 */
public class HashTest
{
    @Test
    public void testSignedUnsignedTrue() throws Exception
    {
        byte[] data1 = {0x62};
        byte[] data2 = {(byte) 0xc3, (byte) 0x97};
        byte[] data3 = {(byte) 0xe2, (byte) 0x99, (byte) 0xa5};
        byte[] data4 = {(byte) 0xe1, (byte) 0x80, (byte) 0xb9, 0x32};
        byte[] data5 = {
                0x01, (byte) 0xc0, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x14, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x04, 0x00,
                0x00, 0x00, 0x00, 0x14,
                0x00, 0x00, 0x00, 0x18,
                0x28, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x02, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
        };
        assertEquals(Hash.hash(new byte[0], 0xbc9f1d34), 0xbc9f1d34);
        assertEquals(Hash.hash(data1, 0xbc9f1d34), 0xef1345c4);
        assertEquals(Hash.hash(data2, 0xbc9f1d34), 0x5b663814);
        assertEquals(Hash.hash(data3, 0xbc9f1d34), 0x323c078f);
        assertEquals(Hash.hash(data4, 0xbc9f1d34), 0xed21633a);
        assertEquals(Hash.hash(data5, 0x12345678), 0xf333dabb);

    }
}
