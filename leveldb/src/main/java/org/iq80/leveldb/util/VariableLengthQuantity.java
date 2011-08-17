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
package org.iq80.leveldb.util;

import org.jboss.netty.buffer.ChannelBuffer;

public final class VariableLengthQuantity
{
    private VariableLengthQuantity() {}

    // todo unroll the loops like coding.cc

    public static void packInt(int numberToCompress, ChannelBuffer buffer) {
        // if key is 0 length
        if (numberToCompress == 0) {
            // write 0 into one byte
            buffer.writeByte((byte)0);
            return;
        }

        while (true) {
            // shift off 7 bits
            int remainder = numberToCompress & 0x7f;
            numberToCompress >>>= 7;

            // if the there are no more 1s in the number, we are done
            if (numberToCompress == 0) {
                // write a positive number to signal we are done
                buffer.writeByte((byte) remainder);
                return;
            }

            // write a negative number to signal there are more bytes to read
            buffer.writeByte((byte) ~remainder);
        }
    }

    public static void packLong(long numberToCompress, ChannelBuffer buffer) {
        // if key is 0 length
        if (numberToCompress == 0) {
            // write 0 into one byte
            buffer.writeByte((byte)0);
            return;
        }

        while (true) {
            // shift off 7 bits
            long remainder = numberToCompress & 0x7f;
            numberToCompress >>>= 7;

            // if the there are no more 1s in the number, we are done
            if (numberToCompress == 0) {
                // write a positive number to signal we are done
                buffer.writeByte((byte) remainder);
                return;
            }

            // write a negative number to signal there are more bytes to read
            buffer.writeByte((byte) ~remainder);
        }
    }

    public static int unpackInt(ChannelBuffer buffer) {
        // number is encoded as blocks of base 128 numbers
        int result = 0;
        for (long index = 0; true; index++) {
            // if the byte is positive, this is the last byte
            int next = buffer.readByte();
            if (next >= 0) {
                // shift the bits to the left and add them to the result
                result ^= next << (7*index);

                return result;
            }
            // flip the bits, shift them to the left, and add them to the result
            result ^= ~next << (7*index);
        }
    }

    public static long unpackLong(ChannelBuffer buffer) {
        // number is encoded as blocks of base 128 numbers
        long result = 0;
        for (long index = 0; true; index++) {
            // if the byte is positive, this is the last byte
            long next = buffer.readByte();
            if (next >= 0) {
                // shift the bits to the left and add them to the result
                result ^= next << (7*index);

                return result;
            }
            // shift the bits to the left and add them to the result
            result ^= ~next << (7*index);
        }
    }
}
