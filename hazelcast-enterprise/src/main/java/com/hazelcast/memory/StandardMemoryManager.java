package com.hazelcast.memory;

import com.hazelcast.monitor.LocalMemoryStats;

/**
 * @author mdogan 03/12/13
 */
public final class StandardMemoryManager implements MemoryManager {

    private final LibMalloc malloc;
    private final OffHeapMemoryStats memoryStats;

    public StandardMemoryManager(MemorySize cap) {
        long size = cap.bytes();
        EnterpriseMemoryStatsSupport.checkFreeMemory(size);
        malloc = new UnsafeMalloc();
        memoryStats = new OffHeapMemoryStats(size);
    }

    StandardMemoryManager(LibMalloc malloc, OffHeapMemoryStats memoryStats) {
        this.malloc = malloc;
        this.memoryStats = memoryStats;
    }

    @Override
    public LocalMemoryStats getMemoryStats() {
        return memoryStats;
    }

    @Override
    public final long allocate(long size) {
        assert size > 0 : "Size must be positive: " + size;
        memoryStats.checkOffHeapAllocation(size);
        long address = malloc.malloc(size);
        memoryStats.addCommittedOffHeap(size);
        return address;
    }

    @Override
    public final void free(long address, long size) {
        assert address > NULL_ADDRESS : "Invalid address: " + address;
        assert size > 0 : "Invalid memory size: " + size;

        malloc.free(address);
        memoryStats.addCommittedOffHeap(-size);
    }

    @Override
    public void compact() {
    }

    @Override
    public long getPage(long address) {
        return address;
    }

    @Override
    public MemoryAllocator unwrapMemoryAllocator() {
        return this;
    }

    @Override
    public void destroy() {
    }
}
