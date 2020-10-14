package com.hazelcast.internal.memory.impl;


import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.PersistentMemoryConfig;

import static com.hazelcast.config.PersistentMemoryMode.MOUNTED;

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
        PersistentMemoryConfig pmemConfig = config.getPersistentMemoryConfig();
        // if the mode is MOUNTED, but the system argument explicitly overrides
        // the config to use Memkind to allocate from DRAM
        if (MOUNTED == pmemConfig.getMode() && !MemkindUtil.useMemkind()) {
            return MemkindPmemMalloc.create(config, size);
        } else {
            return MemkindMalloc.create(config, size);
        }
    }
}
