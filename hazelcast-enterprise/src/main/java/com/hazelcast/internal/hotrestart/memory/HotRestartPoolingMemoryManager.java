package com.hazelcast.internal.hotrestart.memory;

import com.hazelcast.internal.memory.impl.LibMalloc;
import com.hazelcast.internal.memory.impl.LibMallocFactory;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.internal.memory.PooledNativeMemoryStats;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.internal.memory.ThreadLocalPoolingMemoryManager;

/**
 * {@link com.hazelcast.internal.memory.PoolingMemoryManager} specialized for use with the Hot Restart feature.
 */
public class HotRestartPoolingMemoryManager extends PoolingMemoryManager {

    public HotRestartPoolingMemoryManager(
            MemorySize cap, int minBlockSize, int pageSize, float metadataSpacePercentage,
            LibMallocFactory libMallocFactory) {
        super(cap, minBlockSize, pageSize, metadataSpacePercentage, libMallocFactory);
    }

    @Override
    protected ThreadLocalPoolingMemoryManager newThreadLocalPoolingMemoryManager(
            int minBlockSize, int pageSize, LibMalloc malloc, PooledNativeMemoryStats stats) {
        return new HotRestartThreadLocalPoolingMemoryManager(minBlockSize, pageSize, malloc, stats);
    }
}
