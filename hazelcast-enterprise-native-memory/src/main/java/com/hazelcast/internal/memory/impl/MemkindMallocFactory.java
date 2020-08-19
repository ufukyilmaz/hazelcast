package com.hazelcast.internal.memory.impl;


import com.hazelcast.config.NativeMemoryConfig;

/**
 * A factory to create {@link LibMalloc} to manage allocations in non-volatile memory.
 */

public final class MemkindMallocFactory implements LibMallocFactory {

    private final NativeMemoryConfig config;

    public MemkindMallocFactory(NativeMemoryConfig config) {
        this.config = config;
    }

    @Override
    public LibMalloc create(long size) {
        if (!config.getPersistentMemoryConfig().getDirectoryConfigs().isEmpty()) {
            return MemkindPmemMalloc.create(config, size);
        } else {
            return MemkindMalloc.create(config, size);
        }
    }
}
