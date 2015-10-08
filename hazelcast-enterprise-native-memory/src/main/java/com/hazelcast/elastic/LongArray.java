package com.hazelcast.elastic;

import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.nio.UnsafeHelper;

import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;

/**
 *  Native long array.
 */
public class LongArray {

    private final MemoryAllocator malloc;
    private long baseAddress;
    private long length;

    /**
     * @param malloc memory allocator
     * @param length length of array
     */
    public LongArray(MemoryAllocator malloc, long length) {
        this.malloc = malloc;
        this.length = length;
        baseAddress = malloc.allocate(toMemorySize(length));
    }

    private static long toMemorySize(long length) {
        return length * LONG_SIZE_IN_BYTES;
    }

    /**
     * Returns array element at specified index.
     * @param index index of element
     * @return array element at specified index.
     * @throws ArrayIndexOutOfBoundsException if given index is out of bounds
     */
    public long get(long index) {
        return UnsafeHelper.UNSAFE.getLong(baseAddress + offset(index));
    }

    /**
     * Writes value to specified index of array.
     * @param index index to write value
     * @param value value
     * @throws ArrayIndexOutOfBoundsException if given index is out of bounds
     */
    public void set(long index, long value) {
        UnsafeHelper.UNSAFE.putLong(baseAddress + offset(index), value);
    }

    private long offset(long index) {
        if (index < 0 || index >= length) {
            throw new ArrayIndexOutOfBoundsException("Index: " + index + ", Len: " + length);
        }
        return toMemorySize(index);
    }

    /**
     * Expands array to new length
     * @param newLength new length
     */
    public void expand(long newLength) {
        if (newLength == length) {
            return;
        }
        if (newLength < length) {
            throw new IllegalArgumentException("newLength is lower than length! Length: "
                    + length + ", newLength: " + newLength);
        }
        long currentSize = toMemorySize(length);
        long newSize = toMemorySize(newLength);
        baseAddress = malloc.reallocate(baseAddress, currentSize, newSize);
        length = newLength;
    }

    public long address() {
        return baseAddress;
    }

    /**
     * Returns length of array
     * @return length of array
     */
    public long length() {
        return length;
    }

    public void dispose() {
        if (baseAddress != NULL_ADDRESS) {
            malloc.free(baseAddress, toMemorySize(length));
            baseAddress = NULL_ADDRESS;
            length = 0;
        }
    }
}
