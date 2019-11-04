package com.hazelcast.internal.serialization.impl;

import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static com.hazelcast.internal.nio.Bits.BYTE_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.UTF_8;
import static com.hazelcast.internal.nio.Bits.readChar;
import static com.hazelcast.internal.nio.Bits.readInt;
import static com.hazelcast.internal.nio.Bits.readLong;
import static com.hazelcast.internal.nio.Bits.readShort;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Float.floatToIntBits;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FixedBufferObjectDataOutputTest {

    private static final int SIZE = 100;

    @Parameters(name = "bigEndian:{0}")
    public static Collection<Object> data() {
        return Arrays.asList(true, false);
    }

    private FixedBufferObjectDataOutput out;

    @Parameter
    public boolean bigEndian;

    @Before
    public void before() {
        InternalSerializationService mockSerializationService = mock(InternalSerializationService.class);
        ByteOrder byteOrder = bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
        out = new FixedBufferObjectDataOutput(SIZE, mockSerializationService, byteOrder);
    }

    @After
    public void after() {
        out.close();
    }

    @Test
    public void write() {
        byte b = 123;
        out.write(b);
        assertEquals(b, out.getBuffer()[0]);
    }

    @Test
    public void write_withPos() {
        out.write(0);
        out.write(1);
        out.write(2);

        byte b = 123;
        out.write(1, b);

        assertEquals(0, out.getBuffer()[0]);
        assertEquals(b, out.getBuffer()[1]);
        assertEquals(2, out.getBuffer()[2]);
    }

    @Test
    public void write_byteArray() {
        byte[] bytes = new byte[10];
        new Random().nextBytes(bytes);
        out.write(bytes);

        assertArrayEquals(bytes, out.toByteArray());
    }

    @Test
    public void write_byteArray_withOffset() {
        byte[] bytes = new byte[10];
        new Random().nextBytes(bytes);
        out.write(bytes, 1, 5);
        byte[] bytesWritten = new byte[5];
        System.arraycopy(bytes, 1, bytesWritten, 0, 5);

        assertArrayEquals(bytesWritten, out.toByteArray());
    }

    @Test
    public void writeBoolean() {
        out.writeBoolean(true);
        assertEquals(1, out.getBuffer()[0]);
    }

    @Test
    public void writeBoolean_withPos() {
        out.writeByte(0);
        out.writeByte(1);
        out.writeBoolean(0, true);

        assertEquals(1, out.getBuffer()[0]);
        assertEquals(1, out.getBuffer()[1]);
    }

    @Test
    public void writeByte() {
        byte b = 123;
        out.writeByte(b);
        assertEquals(b, out.getBuffer()[0]);
    }

    @Test
    public void writeByte_withPos() {
        out.writeByte(0);
        out.writeByte(1);
        out.writeByte(2);

        byte b = 123;
        out.write(1, b);

        assertEquals(0, out.getBuffer()[0]);
        assertEquals(b, out.getBuffer()[1]);
        assertEquals(2, out.getBuffer()[2]);
    }

    @Test
    public void writeChar() {
        char b = 'X';
        out.writeChar(b);
        assertEquals(b, readChar(out.getBuffer(), 0, bigEndian));
    }

    @Test
    public void writeChar_withPos() {
        char b = 'X';
        out.writeByte(0);
        out.writeChar('a');
        out.writeChar(0, b);
        assertEquals(b, readChar(out.getBuffer(), 0, bigEndian));
    }

    @Test
    public void writeDouble() {
        double v = Math.random();
        out.writeDouble(v);
        assertEquals(doubleToLongBits(v), readLong(out.getBuffer(), 0, bigEndian));
    }

    @Test
    public void writeDouble_withPos() {
        double v = Math.random();
        out.writeLong(123);
        out.writeDouble(0, v);
        assertEquals(doubleToLongBits(v), readLong(out.getBuffer(), 0, bigEndian));
    }

    @Test
    public void writeFloat() {
        float v = (float) Math.random();
        out.writeFloat(v);
        assertEquals(floatToIntBits(v), readInt(out.getBuffer(), 0, bigEndian));
    }

    @Test
    public void writeFloat_withPos() {
        float v = (float) Math.random();
        out.writeInt(123);
        out.writeFloat(0, v);
        assertEquals(floatToIntBits(v), readInt(out.getBuffer(), 0, bigEndian));
    }

    @Test
    public void writeInt() {
        int v = new Random().nextInt();
        out.writeInt(v);
        assertEquals(v, readInt(out.getBuffer(), 0, bigEndian));
    }

    @Test
    public void writeInt_withPos() {
        int v = new Random().nextInt();
        out.writeInt(123);
        out.writeInt(0, v);
        assertEquals(v, readInt(out.getBuffer(), 0, bigEndian));
    }

    @Test
    public void writeLong() {
        long v = new Random().nextLong();
        out.writeLong(v);
        assertEquals(v, readLong(out.getBuffer(), 0, bigEndian));
    }

    @Test
    public void writeLong_withPos() {
        long v = new Random().nextLong();
        out.writeLong(123);
        out.writeLong(0, v);
        assertEquals(v, readLong(out.getBuffer(), 0, bigEndian));
    }

    @Test
    public void writeShort() {
        short v = (short) new Random().nextInt();
        out.writeShort(v);
        assertEquals(v, readShort(out.getBuffer(), 0, bigEndian));
    }

    @Test
    public void writeShort_withPos() {
        short v = (short) new Random().nextInt();
        out.writeShort(123);
        out.writeShort(0, v);
        assertEquals(v, readShort(out.getBuffer(), 0, bigEndian));
    }

    @Test
    public void writeUTF() {
        String s = randomString();
        out.writeUTF(s);

        byte[] bytes = s.getBytes(UTF_8);
        assertEquals(bytes.length, readInt(out.getBuffer(), 0, bigEndian));

        byte[] read = new byte[bytes.length];
        System.arraycopy(out.getBuffer(), INT_SIZE_IN_BYTES, read, 0, bytes.length);
        assertArrayEquals(bytes, read);
    }

    @Test
    public void position() {
        out.write(1);
        assertEquals(1, out.position());
    }

    @Test
    public void setPosition() {
        out.write(new byte[10]);
        out.position(5);
        assertEquals(5, out.position());
    }

    @Test(expected = IllegalArgumentException.class)
    public void setPosition_illegal() {
        out.write(new byte[10]);
        out.position(15);
    }

    @Test
    public void clear() {
        out.write(1);
        out.write(2);
        out.clear();

        assertEquals(0, out.position());
    }

    @Test
    public void close() {
        out.write(1);
        out.write(2);
        out.clear();

        assertEquals(0, out.position());
    }

    @Test
    public void writeBytes_greaterThanCap() {
        byte[] bytes = new byte[out.capacity() + 10];
        new Random().nextBytes(bytes);
        out.write(bytes);

        assertEquals(bytes.length, out.position());
        assertArrayEquals(new byte[out.capacity()], out.getBuffer());
        try {
            out.toByteArray();
            fail();
        } catch (ArrayIndexOutOfBoundsException ignored) {
        }
    }

    @Test
    public void write_greaterThanCap() {
        for (int i = 0; i < 10; i++) {
            out.write(i);
            out.writeInt(i);
            out.writeLong(i);
        }

        assertEquals(10 * (BYTE_SIZE_IN_BYTES + INT_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES), out.position());
        assertEquals(out.capacity(), out.getBuffer().length);
        try {
            out.toByteArray();
            fail();
        } catch (ArrayIndexOutOfBoundsException ignored) {
        }
    }
}
