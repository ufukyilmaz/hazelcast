package com.hazelcast.internal.memory.impl;

import static com.hazelcast.internal.memory.impl.UnsafeUtil.UNSAFE;

public final class UnsafeMalloc implements LibMalloc {

    @Override
    public long malloc(long size) {
        // we depend on returning NULL_ADDRESS if the size is 0
        // Unsafe.allocateMemory() does not guarantee it
        //
        // on JDK implementations that just forward the call to malloc() the
        // result is implementation dependent that may behave as allocating a
        // chunk with the minimum-chunk-size bytes
        //
        // therefore, we perform the check ourselves to guarantee API stability
        if (size == 0) {
            return NULL_ADDRESS;
        }

        try {
            return UNSAFE.allocateMemory(size);
        } catch (OutOfMemoryError e) {
            return NULL_ADDRESS;
        }
    }

    @Override
    public long realloc(long address, long size) {
        // we depend on returning NULL_ADDRESS (and freeing the given memory
        // block) if the size is 0
        // Unsafe.reallocateMemory() does not guarantee this
        //
        // on JDK implementations that just forward the call to realloc() the
        // result is implementation dependent that may behave as reallocating a
        // chunk with the minimum-chunk-size bytes
        //
        // therefore, we perform the check ourselves to guarantee API stability
        if (size == 0) {
            UNSAFE.freeMemory(address);
            return NULL_ADDRESS;
        }

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
