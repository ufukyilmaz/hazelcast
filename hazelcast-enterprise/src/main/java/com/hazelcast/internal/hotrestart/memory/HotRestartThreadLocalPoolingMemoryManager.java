package com.hazelcast.internal.hotrestart.memory;

import com.hazelcast.internal.memory.impl.LibMalloc;
import com.hazelcast.internal.memory.PooledNativeMemoryStats;
import com.hazelcast.internal.memory.ThreadLocalPoolingMemoryManager;

/**
 * Specialization of {@link com.hazelcast.internal.memory.ThreadLocalPoolingMemoryManager} which contributes
 * mutex locking needed to safely perform pointer validation inside
 * {@link com.hazelcast.internal.hotrestart.RamStore#copyEntry(
 * com.hazelcast.internal.hotrestart.KeyHandle, int, com.hazelcast.internal.hotrestart.RecordDataSink)} calls.
 */
public class HotRestartThreadLocalPoolingMemoryManager extends ThreadLocalPoolingMemoryManager {

    private final Object copyEntryMutex = new Object();

    public HotRestartThreadLocalPoolingMemoryManager(
            int minBlockSize, int pageSize, LibMalloc malloc, PooledNativeMemoryStats stats
    ) {
        super(minBlockSize, pageSize, malloc, stats);
    }

    @Override
    public long validateAndGetAllocatedSize(long address) {
        synchronized (copyEntryMutex) {
            return super.validateAndGetAllocatedSize(address);
        }
    }

    @Override
    protected long allocateExternalBlock(long size) {
        synchronized (copyEntryMutex) {
            return super.allocateExternalBlock(size);
        }
    }

    @Override
    protected void freeExternalBlock(long address, long size) {
        synchronized (copyEntryMutex) {
            super.freeExternalBlock(address, size);
        }
    }

    @Override
    protected void onMallocPage(long pageAddress) {
        synchronized (copyEntryMutex) {
            super.onMallocPage(pageAddress);
        }
    }

    @Override
    public void dispose() {
        synchronized (copyEntryMutex) {
            super.dispose();
        }
    }
}
