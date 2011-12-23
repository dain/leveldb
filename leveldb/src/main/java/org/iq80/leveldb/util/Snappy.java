package org.iq80.leveldb.util;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * <p>
 * A Snappy abstraction which attempts uses the iq80 implementation and falls back
 * to the xerial Snappy implementation it cannot be loaded.  You can change the
 * load order by setting the 'leveldb.snappy' system property.  Example:
 *
 * <code>
 * -Dleveldb.snappy=xerial,iq80
 * </code>
 *
 * The system property can also be configured with the name of a class which
 * implements the Snappy.SPI interface.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class Snappy {
    
    public static interface SPI {
        public int uncompress(ByteBuffer compressed, ByteBuffer uncompressed) throws IOException;
        public int uncompress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset) throws IOException;
        public int compress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset) throws IOException;
        public byte[] compress(String text) throws IOException;
        public int maxCompressedLength(int length);
    }

    public static class XerialSnappy implements SPI {
        static {
            // Make sure that the JNI libs are fully loaded.
            try {
                org.xerial.snappy.Snappy.compress("test");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        
        public int uncompress(ByteBuffer compressed, ByteBuffer uncompressed) throws IOException {
            return org.xerial.snappy.Snappy.uncompress(compressed, uncompressed);
        }

        public int uncompress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset) throws IOException {
            return org.xerial.snappy.Snappy.uncompress(input, inputOffset, length, output, outputOffset);
        }

        public int compress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset) throws IOException {
            return org.xerial.snappy.Snappy.compress(input, inputOffset, length, output, outputOffset);
        }

        public byte[] compress(String text) throws IOException {
            return org.xerial.snappy.Snappy.compress(text);
        }

        public int maxCompressedLength(int length) {
            return org.xerial.snappy.Snappy.maxCompressedLength(length);
        }
    }
    
    public static class IQ80Snappy implements SPI {
        static {
            // Make sure that the library can fully load.
            try {
                new IQ80Snappy().compress("test");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public int uncompress(ByteBuffer compressed, ByteBuffer uncompressed) throws IOException {
            byte[] input;
            int inputOffset;
            int length;
            byte[] output;
            int outputOffset;
            if( compressed.hasArray() ) {
                input = compressed.array();
                inputOffset = compressed.arrayOffset() + compressed.position();
                length = compressed.remaining();
            } else {
                input = new byte[compressed.remaining()];
                inputOffset = 0;
                length = input.length;
                compressed.mark();
                compressed.get(input);
                compressed.reset();
            }
            if( uncompressed.hasArray() ) {
                output = uncompressed.array();
                outputOffset = uncompressed.arrayOffset() + uncompressed.position();
            } else {
                output = new byte[uncompressed.capacity()-uncompressed.position()];
                outputOffset = 0;
            }
            int count = org.iq80.snappy.Snappy.uncompress(input, inputOffset, length, output, outputOffset);
            if( uncompressed.hasArray() ) {
                uncompressed.limit(uncompressed.position()+count);
            } else {
                int p = uncompressed.position();
                uncompressed.limit(uncompressed.capacity());
                uncompressed.put(output, 0, count);
                uncompressed.flip().position(p);
            }
            return count;
        }

        public int uncompress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset) throws IOException {
            return  org.iq80.snappy.Snappy.uncompress(input, inputOffset, length, output, outputOffset);
        }

        public int compress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset) throws IOException {
            return org.iq80.snappy.Snappy.compress(input, inputOffset, length, output, outputOffset);
        }

        public byte[] compress(String text) throws IOException {
            byte[] uncomressed = text.getBytes("UTF-8");
            byte[] compressedOut = new byte[maxCompressedLength(uncomressed.length)];
            int compressedSize = compress(uncomressed, 0, uncomressed.length, compressedOut, 0);
            byte[] trimmedBuffer = new byte[compressedSize];
            System.arraycopy(compressedOut, 0, trimmedBuffer, 0, compressedSize);
            return trimmedBuffer;
        }

        public int maxCompressedLength(int length) {
            return org.iq80.snappy.Snappy.maxCompressedLength(length);
        }
    }

    static final private SPI SNAPPY;
    static {
        SPI attempt = null;
        String[] factories = System.getProperty("leveldb.snappy", "iq80,xerial").split(",");
        for (int i = 0; i < factories.length && attempt==null; i++) {
            String name = factories[i];
            try {
                name = name.trim();
                if("xerial".equals(name.toLowerCase())) {
                    name = "org.iq80.leveldb.util.Snappy$XerialSnappy";
                } else if("iq80".equals(name.toLowerCase())) {
                    name = "org.iq80.leveldb.util.Snappy$IQ80Snappy";
                }
                attempt = (SPI) Thread.currentThread().getContextClassLoader().loadClass(name).newInstance();
            } catch (Throwable e) {
            }
        }
        SNAPPY = attempt;
    }


    public static boolean available() {
        return SNAPPY !=null;
    }

    public static void uncompress(ByteBuffer compressed, ByteBuffer uncompressed) throws IOException {
        SNAPPY.uncompress(compressed, uncompressed);
    }

    public static void uncompress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset) throws IOException {
        SNAPPY.uncompress(input, inputOffset, length, output, outputOffset);
    }

    public static int compress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset) throws IOException {
        return SNAPPY.compress(input, inputOffset, length, output, outputOffset);
    }

    public static byte[] compress(String text) throws IOException {
        return SNAPPY.compress(text);
    }

    public static int maxCompressedLength(int length) {
        return SNAPPY.maxCompressedLength(length);
    }

}
