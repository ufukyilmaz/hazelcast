package com.hazelcast.internal.elastic;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.nio.Disposable;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;

/**
 *  Native long array.
 */
public final class LongArray implements Disposable {

    private final MemoryAllocator malloc;
    private final MemoryAccessor mem;
    private long baseAddress;
    private long length;

    /**
     * @param memMgr memory allocator
     * @param length length of array
     */
    public LongArray(MemoryManager memMgr, long length) {
        this.malloc = memMgr.getAllocator();
        this.mem = memMgr.getAccessor();
        this.length = length;
        baseAddress = length > 0 ? malloc.allocate(lengthInBytes(length)) : NULL_ADDRESS;
    }

    /**
     * Returns array element at specified index.
     * @param index index of element
     * @return array element at specified index.
     */
    public long get(long index) {
        return mem.getLong(addressOfElement(index));
    }

    /**
     * Writes value to specified index of array.
     * @param index index to write value
     * @param value value
     */
    public void set(long index, long value) {
        mem.putLong(addressOfElement(index), value);
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
        assert baseAddress != NULL_ADDRESS;
        assert index >= 0 && index < length : "Native array index out of bounds: " + index + " vs. length " + length;
        return baseAddress + lengthInBytes(index);
    }

    private static long lengthInBytes(long lengthInElements) {
        return lengthInElements * LONG_SIZE_IN_BYTES;
    }
}
