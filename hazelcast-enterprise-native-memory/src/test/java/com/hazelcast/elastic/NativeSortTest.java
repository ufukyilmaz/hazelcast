package com.hazelcast.elastic;

import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import sun.misc.Unsafe;

import java.util.Arrays;
import java.util.Random;

import static com.hazelcast.elastic.NativeSort.quickSortInt;
import static com.hazelcast.elastic.NativeSort.quickSortLong;
import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NativeSortTest {

    private static final Unsafe unsafe = UnsafeHelper.UNSAFE;
    private static final int LEN = 10000;

    private long arrayAddress;

    @After
    public void dispose() {
        if (arrayAddress != NULL_ADDRESS) {
            unsafe.freeMemory(arrayAddress);
        }
    }

    @Test
    public void testQuickSortInt() throws Exception {
        final int[] array = newIntArray();

        final int length = array.length;
        final int[] sorted = Arrays.copyOf(array, length);
        Arrays.sort(sorted);

        arrayAddress = unsafe.allocateMemory(length * INT_SIZE_IN_BYTES);
        unsafe.copyMemory(array, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET, null,
                arrayAddress, length * INT_SIZE_IN_BYTES);

        quickSortInt(arrayAddress, length);
        verify(sorted, arrayAddress, length);
    }

    @Test
    public void testQuickSortLong() throws Exception {
        final long[] array = newLongArray();

        final int length = array.length;
        final long[] sorted = Arrays.copyOf(array, length);
        Arrays.sort(sorted);

        arrayAddress = unsafe.allocateMemory(length * LONG_SIZE_IN_BYTES);
        unsafe.copyMemory(array, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET, null,
                arrayAddress, length * LONG_SIZE_IN_BYTES);

        quickSortLong(arrayAddress, length);
        verify(sorted, arrayAddress, length);
    }

    private static void verify(int[] array, long address, int length) {
        for (int i = 0; i < length; i++) {
            int k = unsafe.getInt(address + i * INT_SIZE_IN_BYTES);
            assertEquals(array[i], k);
        }
    }

    private static int[] newIntArray() {
        int[] array = new int[LEN];
        Random random = new Random();
        for (int i = 0; i < array.length; i++) {
            array[i] = random.nextInt();
        }
        return array;
    }

    private static void verify(long[] array, long address, int length) {
        for (int i = 0; i < length; i++) {
            long k = unsafe.getLong(address + i * LONG_SIZE_IN_BYTES);
            assertEquals(array[i], k);
        }
    }

    private static long[] newLongArray() {
        long[] array = new long[LEN];
        Random random = new Random();
        for (int i = 0; i < array.length; i++) {
            array[i] = random.nextLong();
        }
        return array;
    }
}
