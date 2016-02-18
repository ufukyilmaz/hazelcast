package com.hazelcast.elastic;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryAccessorProvider;
import com.hazelcast.internal.memory.MemoryAccessorType;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;

/**
 * Quick sort algorithm implementations for native {@code int}
 * {@code long} arrays.
 */
public final class NativeSort {

    // We are using `STANDARD` memory accessor because we internally guarantee that
    // every memory access is aligned.
    private static final MemoryAccessor MEMORY_ACCESSOR =
            MemoryAccessorProvider.getMemoryAccessor(MemoryAccessorType.STANDARD);

    private NativeSort() { }

    /**
     * Sorts the native int array into ascending numerical order
     * using quick sort.
     *
     * @param address base address of the array
     * @param length length of the array
     */
    public static void quickSortInt(long address, long length) {
        quickSortInt0(address, 0, length - 1);
    }

    /**
     * Sorts the native long array into ascending numerical order
     * using quick sort.
     *
     * @param address base address of the array
     * @param length length of the array
     */
    public static void quickSortLong(long address, long length) {
        quickSortLong0(address, 0, length - 1);
    }

    private static void quickSortInt0(long address, long left, long right) {
        long index = partitionInt(address, left, right);
        if (left < index - 1) {
            quickSortInt0(address, left, index - 1);
        }
        if (index < right) {
            quickSortInt0(address, index, right);
        }
    }

    private static long partitionInt(long address, long left, long right) {
        long i = left;
        long j = right;
        long offset = ((left + right) >>> 1) * INT_SIZE_IN_BYTES;
        int pivot = MEMORY_ACCESSOR.getInt(address + offset);
        int tmp;

        while (i <= j) {
            while (MEMORY_ACCESSOR.getInt(address + i * INT_SIZE_IN_BYTES) < pivot) {
                i++;
            }
            while (MEMORY_ACCESSOR.getInt(address + j * INT_SIZE_IN_BYTES) > pivot) {
                j--;
            }

            if (i <= j) {
                tmp = MEMORY_ACCESSOR.getInt(address + i * INT_SIZE_IN_BYTES);
                MEMORY_ACCESSOR.putInt(address + i * INT_SIZE_IN_BYTES, MEMORY_ACCESSOR.getInt(address + j * INT_SIZE_IN_BYTES));
                MEMORY_ACCESSOR.putInt(address + j * INT_SIZE_IN_BYTES, tmp);
                i++;
                j--;
            }
        }
        return i;
    }

    private static void quickSortLong0(long address, long left, long right) {
        long index = partitionLong(address, left, right);
        if (left < index - 1) {
            quickSortLong0(address, left, index - 1);
        }
        if (index < right) {
            quickSortLong0(address, index, right);
        }
    }

    private static long partitionLong(long address, long left, long right) {
        long i = left;
        long j = right;
        long offset = ((left + right) >>> 1) * LONG_SIZE_IN_BYTES;
        long pivot = MEMORY_ACCESSOR.getLong(address + offset);
        long tmp;

        while (i <= j) {
            while (MEMORY_ACCESSOR.getLong(address + i * LONG_SIZE_IN_BYTES) < pivot) {
                i++;
            }
            while (MEMORY_ACCESSOR.getLong(address + j * LONG_SIZE_IN_BYTES) > pivot) {
                j--;
            }

            if (i <= j) {
                tmp = MEMORY_ACCESSOR.getLong(address + i * LONG_SIZE_IN_BYTES);
                MEMORY_ACCESSOR.putLong(address + i * LONG_SIZE_IN_BYTES,
                                        MEMORY_ACCESSOR.getLong(address + j * LONG_SIZE_IN_BYTES));
                MEMORY_ACCESSOR.putLong(address + j * LONG_SIZE_IN_BYTES, tmp);
                i++;
                j--;
            }
        }
        return i;
    }
}
