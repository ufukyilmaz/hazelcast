package com.hazelcast.elastic;

import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.nio.Disposable;

import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.UnsafeHelper.UNSAFE;

/**
 *  Native long array.
 */
public final class LongArray implements Disposable {

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
        baseAddress = malloc.allocate(lengthInBytes(length));
    }

    /**
     * Returns array element at specified index.
     * @param index index of element
     * @return array element at specified index.
     */
    public long get(long index) {
        return UNSAFE.getLong(addressOfElement(index));
    }

    /**
     * Writes value to specified index of array.
     * @param index index to write value
     * @param value value
     */
    public void set(long index, long value) {
        UNSAFE.putLong(addressOfElement(index), value);
    }

    /**
     * Expands array to new length
     * @param newLength new length
     */
    public void expand(long newLength) {
        if (newLength == length) {
            return;
        }
        assert newLength > length
                : String.format("Requested to expand to smaller length. Current %,d, requested %,d", length, newLength);
        long currentSize = lengthInBytes(length);
        long newSize = lengthInBytes(newLength);
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
            malloc.free(baseAddress, lengthInBytes(length));
            baseAddress = NULL_ADDRESS;
            length = 0;
        }
    }

    private long addressOfElement(long index) {
        assert index >= 0 && index < length : "Native array index out of bounds: " + index + " vs. length " + length;
        return baseAddress + lengthInBytes(index);
    }

    private static long lengthInBytes(long lengthInElements) {
        return lengthInElements * LONG_SIZE_IN_BYTES;
    }
}
