package com.hazelcast.internal.memory.impl;


import com.hazelcast.config.NativeMemoryConfig;

/**
 * A factory to create {@link LibMalloc} to manage allocations in non-volatile memory.
 */

public final class PersistentMemoryMallocFactory implements LibMallocFactory {

    private final NativeMemoryConfig config;

    public PersistentMemoryMallocFactory(NativeMemoryConfig config) {
        this.config = config;
    }

    @Override
    public LibMalloc create(long size) {
        return new PersistentMemoryMalloc(config, size);
    }
}
