package org.iq80.leveldb.util;

import com.google.common.base.Function;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import static org.iq80.leveldb.util.PureJavaCrc32C.mask;
import static org.iq80.leveldb.util.PureJavaCrc32C.unmask;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class PureJavaCrc32CTest
{
    @Test(dataProvider = "crcs")
    public void testCrc(int expectedCrc, byte[] data)
    {
        assertEquals(expectedCrc, computeCrc(data));
    }

    @DataProvider(name = "crcs")
    public Object[][] data()
    {
        return new Object[][] {
                new Object[] { 0x8a9136aa, arrayOf(32, (byte) 0) },
                new Object[] { 0x62a8ab43, arrayOf(32, (byte) 0xff) },
                new Object[] { 0x46dd794e, arrayOf(32, new Function<Integer, Byte>()
                {
                    public Byte apply(Integer position)
                    {
                        return (byte) position.intValue();
                    }
                }) },
                new Object[] { 0x113fdb5c, arrayOf(32, new Function<Integer, Byte>()
                {
                    public Byte apply(Integer position)
                    {
                        return (byte) (31 - position);
                    }
                }) },
                new Object[] { 0xd9963a56, arrayOf(new int[] {
                        0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
                        0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }) }
        };
    }

    @Test
    public void testProducesDifferentCrcs()
            throws UnsupportedEncodingException
    {
        assertFalse(computeCrc("a".getBytes("ASCII")) == computeCrc("foo".getBytes("ASCII")));
    }

    @Test
    public void testComposes()
            throws UnsupportedEncodingException
    {
        PureJavaCrc32C crc = new PureJavaCrc32C();
        crc.update("hello ".getBytes("ASCII"), 0, 6);
        crc.update("world".getBytes("ASCII"), 0, 5);

        assertEquals(crc.getIntValue(), computeCrc("hello world".getBytes("ASCII")));
    }

    @Test
    public void testMask()
            throws UnsupportedEncodingException
    {
        PureJavaCrc32C crc = new PureJavaCrc32C();
        crc.update("foo".getBytes("ASCII"), 0, 3);

        assertEquals(crc.getMaskedValue(), PureJavaCrc32C.mask(crc.getIntValue()));
        assertFalse(crc.getIntValue() == crc.getMaskedValue(), "crc should not match masked crc");
        assertFalse(crc.getIntValue() == PureJavaCrc32C.mask(crc.getMaskedValue()), "crc should not match double masked crc");
        assertEquals(crc.getIntValue(), unmask(crc.getMaskedValue()));
        assertEquals(crc.getIntValue(), unmask(unmask(mask(crc.getMaskedValue()))));
    }

    private int computeCrc(byte[] data)
    {
        PureJavaCrc32C crc = new PureJavaCrc32C();
        crc.update(data, 0, data.length);
        return crc.getIntValue();
    }

    private byte[] arrayOf(int size, byte value)
    {
        byte[] result = new byte[size];
        Arrays.fill(result, value);
        return result;
    }

    private byte[] arrayOf(int size, Function<Integer, Byte> generator)
    {
        byte[] result = new byte[size];
        for (int i = 0; i < result.length; ++i) {
            result[i] = generator.apply(i);
        }

        return result;
    }

    private byte[] arrayOf(int[] bytes)
    {
        byte[] result = new byte[bytes.length];
        for (int i = 0; i < result.length; ++i) {
            result[i] = (byte) bytes[i];
        }

        return result;
    }

}
