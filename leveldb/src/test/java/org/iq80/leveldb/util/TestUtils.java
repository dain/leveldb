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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static java.nio.charset.StandardCharsets.US_ASCII;

public final class TestUtils
{
    private TestUtils()
    {
        //utility
    }

    public static Slice randomString(Random rnd, int len)
    {
        final byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte) (' ' + rnd.nextInt(95));   // ' ' .. '~'
        }
        return new Slice(bytes);
    }

    public static byte[] randomKey(Random rnd, int len)
    {
        // Make sure to generate a wide variety of characters so we
        // test the boundary conditions for short-key optimizations.
        byte[] kTestChars = {
                0, 1, 'a', 'b', 'c', 'd', 'e', (byte) 0xfd, (byte) 0xfe, (byte) 0xff
        };
        byte[] result = new byte[len];
        for (int i = 0; i < len; i++) {
            result[i] = kTestChars[rnd.nextInt(kTestChars.length)];
        }
        return result;
    }

    public static Slice compressibleString(Random rnd, double compressedFraction, int len) throws IOException
    {
        int raw = (int) (len * compressedFraction);
        if (raw < 1) {
            raw = 1;
        }
        final byte[] bytes = randomString(rnd, raw).getBytes();

        final ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream(len);
        while (byteOutputStream.size() < len) {
            byteOutputStream.write(bytes);
        }
        final Slice slice = new Slice(byteOutputStream.toByteArray());
        byteOutputStream.close();
        return slice;
    }

    public static String longString(int length, char character)
    {
        char[] chars = new char[length];
        Arrays.fill(chars, character);
        return new String(chars);
    }

    public static Slice asciiToSlice(String value)
    {
        return Slices.copiedBuffer(value, US_ASCII);
    }

    public static byte[] asciiToBytes(String value)
    {
        return asciiToSlice(value).getBytes();
    }
}
