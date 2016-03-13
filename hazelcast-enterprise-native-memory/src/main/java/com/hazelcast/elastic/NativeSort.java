package com.hazelcast.elastic;

import com.hazelcast.spi.impl.memory.IntMemArrayQuickSorter;
import com.hazelcast.spi.impl.memory.LongMemArrayQuickSorter;

import static com.hazelcast.spi.memory.GlobalMemoryAccessorRegistry.AMEM;

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
        new IntMemArrayQuickSorter(AMEM, address).sort(0, length);
    }

    /**
     * Sorts the native long array into ascending numerical order
     * using quick sort.
     *
     * @param address base address of the array
     * @param length length of the array
     */
    public static void quickSortLong(long address, long length) {
        new LongMemArrayQuickSorter(AMEM, address).sort(0, length);
    }
}
