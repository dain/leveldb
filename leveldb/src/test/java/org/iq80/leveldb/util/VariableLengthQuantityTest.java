package org.iq80.leveldb.util;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class VariableLengthQuantityTest
{
    @Test
    public void testWriteVariableLengthInt()
    {
        testVariableLengthInt(0x0);
        testVariableLengthInt(0xf);
        testVariableLengthInt(0xff);
        testVariableLengthInt(0xfff);
        testVariableLengthInt(0xffff);
        testVariableLengthInt(0xfffff);
        testVariableLengthInt(0xffffff);
        testVariableLengthInt(0xfffffff);
        testVariableLengthInt(0xffffffff);
    }

    private void testVariableLengthInt(int value)
    {
        SliceOutput output = Slices.allocate(5).output();
        VariableLengthQuantity.writeVariableLengthInt(value, output);
        assertEquals(output.size(), VariableLengthQuantity.variableLengthSize(value));
        int actual = VariableLengthQuantity.readVariableLengthInt(output.slice().input());
        assertEquals(actual, value);
    }

    @Test
    public void testWriteVariableLengthLong()
    {
        testVariableLengthLong(0x0L);
        testVariableLengthLong(0xfL);
        testVariableLengthLong(0xffL);
        testVariableLengthLong(0xfffL);
        testVariableLengthLong(0xffffL);
        testVariableLengthLong(0xfffffL);
        testVariableLengthLong(0xffffffL);
        testVariableLengthLong(0xfffffffL);
        testVariableLengthLong(0xffffffffL);
        testVariableLengthLong(0xfffffffffL);
        testVariableLengthLong(0xffffffffffL);
        testVariableLengthLong(0xfffffffffffL);
        testVariableLengthLong(0xffffffffffffL);
        testVariableLengthLong(0xfffffffffffffL);
        testVariableLengthLong(0xffffffffffffffL);
        testVariableLengthLong(0xfffffffffffffffL);
        testVariableLengthLong(0xffffffffffffffffL);
    }

    private void testVariableLengthLong(long value)
    {
        SliceOutput output = Slices.allocate(12).output();
        VariableLengthQuantity.writeVariableLengthLong(value, output);
        assertEquals(output.size(), VariableLengthQuantity.variableLengthSize(value));
        long actual = VariableLengthQuantity.readVariableLengthLong(output.slice().input());
        assertEquals(actual, value);
    }
}
