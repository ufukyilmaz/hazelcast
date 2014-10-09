package com.hazelcast.memory;

import com.hazelcast.nio.UnsafeHelper;

/**
 * @author mdogan 03/12/13
 */
public final class UnsafeMalloc implements LibMalloc {

    public long malloc(long size) {
        return UnsafeHelper.UNSAFE.allocateMemory(size);
    }

    public void free(long address) {
        UnsafeHelper.UNSAFE.freeMemory(address);
    }

    @Override
    public String toString() {
        return "UnsafeMalloc";
    }
}
