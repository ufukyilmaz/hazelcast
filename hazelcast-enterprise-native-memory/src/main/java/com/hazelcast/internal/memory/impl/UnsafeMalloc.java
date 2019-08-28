package com.hazelcast.internal.memory.impl;

import static com.hazelcast.internal.memory.impl.UnsafeUtil.UNSAFE;

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

    @Override
    public void dispose() {
        // no-op
    }
}
