package com.hazelcast.memory;

import com.hazelcast.nio.Bits;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteOrder;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * @author mdogan 02/06/14
 */

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MemoryBlockTest {

    private static final LibMalloc MALLOC = new UnsafeMalloc();
    private static final boolean BIG_ENDIAN = ByteOrder.BIG_ENDIAN == ByteOrder.nativeOrder();

    private MemoryBlock block;

    @Before
    public void create() {
        int size = (int) MemoryUnit.KILOBYTES.toBytes(1);
        long address = MALLOC.malloc(size);
        block = new MemoryBlock(address, size);
    }

    @After
    public void destroy() {
        MALLOC.free(block.address());
    }

    @Test
    public void testReadWrite() {
        int totalSize = block.size();
        // long + double + int + float + short + char + byte = 29
        int chunk = 29;
        int count = totalSize / chunk;
        int offset = 0;
        for (int i = 0; i < count; i++) {
            block.writeLong(offset, i);
            block.writeDouble(offset + 8, i);
            block.writeInt(offset + 16, i);
            block.writeFloat(offset + 20, i);
            block.writeShort(offset + 24, (short) i);
            block.writeChar(offset + 26, (char) i);
            block.writeByte(offset + 28, (byte) i);
            offset += chunk;
        }

        offset = 0;
        for (int i = 0; i < count; i++) {
            assertEquals(i, block.readLong(offset));
            assertEquals(i, block.readDouble(offset + 8), 0);
            assertEquals(i, block.readInt(offset + 16));
            assertEquals(i, block.readFloat(offset + 20), 0);
            assertEquals(i, block.readShort(offset + 24));
            assertEquals(i, block.readChar(offset + 26));
            assertEquals(i, block.readByte(offset + 28));
            offset += chunk;
        }
    }

    @Test
    public void testCopyTo() {
        Random rand = new Random();
        byte[] reference = new byte[block.size()];
        boolean bigEndian = ByteOrder.BIG_ENDIAN == ByteOrder.nativeOrder();

        int count = 100;
        for (int i = 0; i < count; i++) {
            int offset = i * 8;
            long value = rand.nextLong();

            block.writeLong(offset, value);
            Bits.writeLong(reference, offset, value, bigEndian);
        }

        byte[] buffer = new byte[block.size()];
        block.copyTo(0, buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET, count * 8);

        Assert.assertArrayEquals(reference, buffer);
    }

    @Test
    public void testCopyToByteArray() {
        Random rand = new Random();

        byte[] reference = new byte[block.size()];

        int count = 100;
        for (int i = 0; i < count; i++) {
            int offset = i * 8;
            long value = rand.nextLong();

            block.writeLong(offset, value);
            Bits.writeLong(reference, offset, value, BIG_ENDIAN);
        }

        byte[] buffer = new byte[block.size()];
        block.copyToByteArray(0, buffer, 0, count * 8);

        Assert.assertArrayEquals(reference, buffer);
    }

    @Test
    public void testCopyFrom() {
        Random rand = new Random();
        byte[] reference = new byte[block.size()];
        rand.nextBytes(reference);

        block.copyFrom(0, reference, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET, reference.length);

        int count = block.size() / 8;
        for (int i = 0; i < count; i++) {
            int offset = i * 8;
            assertEquals(Bits.readLong(reference, offset, BIG_ENDIAN), block.readLong(offset));
        }
    }

    @Test
    public void testCopyFromByteArray() {
        Random rand = new Random();
        byte[] reference = new byte[block.size()];
        rand.nextBytes(reference);

        block.copyFromByteArray(0, reference, 0, reference.length);

        int count = block.size() / 8;
        for (int i = 0; i < count; i++) {
            int offset = i * 8;
            assertEquals(Bits.readLong(reference, offset, BIG_ENDIAN), block.readLong(offset));
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testIllegalWriteLong() {
        block.writeLong(block.size() - 4, 123456L);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testIllegalWriteDouble() {
        block.writeDouble(block.size() - 4, 123456.789);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testIllegalWriteInt() {
        block.writeInt(block.size() - 3, 123);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testIllegalWriteFloat() {
        block.writeFloat(block.size() - 3, 123.456f);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testIllegalWriteShort() {
        block.writeShort(block.size() - 1, (short) 123);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testIllegalWriteChar() {
        block.writeChar(block.size() - 1, 'X');
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testIllegalWriteByte() {
        block.writeByte(block.size(), (byte) 111);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testIllegalReadLong() {
        block.readLong(block.size() - 4);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testIllegalReadDouble() {
        block.readDouble(block.size() - 4);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testIllegalReadInt() {
        block.readInt(block.size() - 3);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testIllegalReadFloat() {
        block.readFloat(block.size() - 3);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testIllegalReadShort() {
        block.readShort(block.size() - 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testIllegalReadChar() {
        block.readChar(block.size() - 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testIllegalReadByte() {
        block.readByte(block.size());
    }
}
