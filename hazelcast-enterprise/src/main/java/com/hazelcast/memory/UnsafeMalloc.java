package com.hazelcast.memory;

import com.hazelcast.nio.UnsafeHelper;

/**
 * @author mdogan 03/12/13
 */
public final class UnsafeMalloc implements LibMalloc {

    @Override
    public long malloc(long size) {
        return UnsafeHelper.UNSAFE.allocateMemory(size);
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
