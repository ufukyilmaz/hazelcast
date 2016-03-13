package com.hazelcast.spi.impl.memory;

import static com.hazelcast.spi.impl.memory.UnsafeUtil.UNSAFE;

/**
 * @author mdogan 03/12/13
 */
public final class UnsafeMalloc implements LibMalloc {

    @Override
    public long malloc(long size) {
        try {
            return UNSAFE.allocateMemory(size);
        } catch (OutOfMemoryError e) {
            return NULL_ADDRESS;
        }
    }

    @Override
    public long realloc(long address, long size) {
        try {
            return UNSAFE.reallocateMemory(address, size);
        } catch (OutOfMemoryError e) {
            return NULL_ADDRESS;
        }
    }

    @Override
    public void free(long address) {
        UNSAFE.freeMemory(address);
    }

    @Override
    public String toString() {
        return "UnsafeMalloc";
    }
}
