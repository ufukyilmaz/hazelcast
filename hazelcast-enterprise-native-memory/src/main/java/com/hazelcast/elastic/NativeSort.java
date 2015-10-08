package com.hazelcast.elastic;

import com.hazelcast.nio.UnsafeHelper;
import sun.misc.Unsafe;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;

/**
 * Quick sort algorithm implementations for native {@code int}
 * {@code long} arrays.
 */
public final class NativeSort {

    private static final Unsafe unsafe = UnsafeHelper.UNSAFE;

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
        long i = left, j = right;
        long offset = ((left + right) >> 1) * INT_SIZE_IN_BYTES;
        int pivot = unsafe.getInt(address + offset);
        int tmp;

        while (i <= j) {
            while (unsafe.getInt(address + i * INT_SIZE_IN_BYTES) < pivot) {
                i++;
            }
            while (unsafe.getInt(address + j * INT_SIZE_IN_BYTES) > pivot) {
                j--;
            }

            if (i <= j) {
                tmp = unsafe.getInt(address + i * INT_SIZE_IN_BYTES);
                unsafe.putInt(address + i * INT_SIZE_IN_BYTES, unsafe.getInt(address + j * INT_SIZE_IN_BYTES));
                unsafe.putInt(address + j * INT_SIZE_IN_BYTES, tmp);
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
        long i = left, j = right;
        long offset = ((left + right) >> 1) * LONG_SIZE_IN_BYTES;
        long pivot = unsafe.getLong(address + offset);
        long tmp;

        while (i <= j) {
            while (unsafe.getLong(address + i * LONG_SIZE_IN_BYTES) < pivot) {
                i++;
            }
            while (unsafe.getLong(address + j * LONG_SIZE_IN_BYTES) > pivot) {
                j--;
            }

            if (i <= j) {
                tmp = unsafe.getLong(address + i * LONG_SIZE_IN_BYTES);
                unsafe.putLong(address + i * LONG_SIZE_IN_BYTES, unsafe.getLong(address + j * LONG_SIZE_IN_BYTES));
                unsafe.putLong(address + j * LONG_SIZE_IN_BYTES, tmp);
                i++;
                j--;
            }
        }
        return i;
    }
}
