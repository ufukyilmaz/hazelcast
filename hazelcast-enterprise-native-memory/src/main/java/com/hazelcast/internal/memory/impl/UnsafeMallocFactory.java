package com.hazelcast.internal.memory.impl;

import com.hazelcast.memory.FreeMemoryChecker;

/**
 * A factory to create {@link LibMalloc} to manage allocations in native volatile memory.
 */
public class UnsafeMallocFactory implements LibMallocFactory {

    private final FreeMemoryChecker freeMemoryChecker;

    public UnsafeMallocFactory(FreeMemoryChecker freeMemoryChecker) {
        this.freeMemoryChecker = freeMemoryChecker;
    }

    @Override
    public LibMalloc create(long size) {
        freeMemoryChecker.checkFreeMemory(size);
        return new UnsafeMalloc();
    }
}
