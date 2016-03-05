package com.hazelcast.memory;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.nio.Disposable;

/**
 * Contract to allocate and access memory in an abstract address space, which is not necessarily the
 * underlying CPU's native address space. Memory allocated from the {@link MemoryAllocator} must be
 * accessed through the {@link MemoryAccessor}.
 */
public interface MemoryManager extends Disposable {

    /** @return the associated {@link MemoryAllocator} */
    MemoryAllocator getAllocator();

    /** @return the associated {@link MemoryAccessor} */
    MemoryAccessor getAccessor();
}
