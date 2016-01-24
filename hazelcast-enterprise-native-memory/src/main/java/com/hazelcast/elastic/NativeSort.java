package com.hazelcast.elastic;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.UnsafeHelper.UNSAFE;

/**
 * Quick sort algorithm implementations for native {@code int}
 * {@code long} arrays.
 */
public final class NativeSort {

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
        long offset = ((left + right) >> 1) * INT_SIZE_IN_BYTES;
        int pivot = UNSAFE.getInt(address + offset);
        int tmp;

        while (i <= j) {
            while (UNSAFE.getInt(address + i * INT_SIZE_IN_BYTES) < pivot) {
                i++;
            }
            while (UNSAFE.getInt(address + j * INT_SIZE_IN_BYTES) > pivot) {
                j--;
            }

            if (i <= j) {
                tmp = UNSAFE.getInt(address + i * INT_SIZE_IN_BYTES);
                UNSAFE.putInt(address + i * INT_SIZE_IN_BYTES, UNSAFE.getInt(address + j * INT_SIZE_IN_BYTES));
                UNSAFE.putInt(address + j * INT_SIZE_IN_BYTES, tmp);
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
        long offset = ((left + right) >> 1) * LONG_SIZE_IN_BYTES;
        long pivot = UNSAFE.getLong(address + offset);
        long tmp;

        while (i <= j) {
            while (UNSAFE.getLong(address + i * LONG_SIZE_IN_BYTES) < pivot) {
                i++;
            }
            while (UNSAFE.getLong(address + j * LONG_SIZE_IN_BYTES) > pivot) {
                j--;
            }

            if (i <= j) {
                tmp = UNSAFE.getLong(address + i * LONG_SIZE_IN_BYTES);
                UNSAFE.putLong(address + i * LONG_SIZE_IN_BYTES, UNSAFE.getLong(address + j * LONG_SIZE_IN_BYTES));
                UNSAFE.putLong(address + j * LONG_SIZE_IN_BYTES, tmp);
                i++;
                j--;
            }
        }
        return i;
    }
}
