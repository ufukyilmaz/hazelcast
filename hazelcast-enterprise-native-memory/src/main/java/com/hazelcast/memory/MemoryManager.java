package com.hazelcast.memory;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.nio.Disposable;

/**
 * Javadoc pending.
 */
public interface MemoryManager extends Disposable {

    MemoryAllocator getAllocator();

    MemoryAccessor getAccessor();

    MemoryStats getMemoryStats();
}
