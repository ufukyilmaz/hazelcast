package com.hazelcast.memory;

import static com.hazelcast.memory.FreeMemoryChecker.checkFreeMemory;

/**
 * @author mdogan 03/12/13
 */
public final class StandardMemoryManager implements MemoryManager {

    private final LibMalloc malloc;
    private final NativeMemoryStats memoryStats;

    public StandardMemoryManager(MemorySize cap) {
        long size = cap.bytes();
        checkFreeMemory(size);
        malloc = new UnsafeMalloc();
        memoryStats = new NativeMemoryStats(size);
    }

    StandardMemoryManager(LibMalloc malloc, NativeMemoryStats memoryStats) {
        this.malloc = malloc;
        this.memoryStats = memoryStats;
    }

    @Override
    public MemoryStats getMemoryStats() {
        return memoryStats;
    }

    @Override
    public final long allocate(long size) {
        assert size > 0 : "Size must be positive: " + size;
        memoryStats.checkCommittedNative(size);
        long address = malloc.malloc(size);
        memoryStats.addCommittedNative(size);
        return address;
    }

    @Override
    public final void free(long address, long size) {
        assert address > NULL_ADDRESS : "Invalid address: " + address;
        assert size > 0 : "Invalid memory size: " + size;

        malloc.free(address);
        memoryStats.addCommittedNative(-size);
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
    public boolean isDestroyed() {
        return false;
    }

    @Override
    public void destroy() {
    }

    @Override
    public int getSize(long address) {
        return SIZE_INVALID;
    }

}
