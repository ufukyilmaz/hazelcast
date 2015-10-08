package com.hazelcast.memory;

import com.hazelcast.nio.UnsafeHelper;

/**
 * @author mdogan 03/12/13
 */
public final class UnsafeMalloc implements LibMalloc {

    @Override
    public long malloc(long size) {
        try {
            return UnsafeHelper.UNSAFE.allocateMemory(size);
        } catch (OutOfMemoryError e) {
            return NULL_ADDRESS;
        }
    }

    @Override
    public long realloc(long address, long size) {
        try {
            return UnsafeHelper.UNSAFE.reallocateMemory(address, size);
        } catch (OutOfMemoryError e) {
            return NULL_ADDRESS;
        }
    }

    @Override
    public void free(long address) {
        UnsafeHelper.UNSAFE.freeMemory(address);
    }

    @Override
    public String toString() {
        return "UnsafeMalloc";
    }
}
