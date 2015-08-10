package com.hazelcast.memory;

import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.util.collection.Long2LongHashMap;
import com.hazelcast.util.function.LongLongConsumer;

import static com.hazelcast.memory.FreeMemoryChecker.checkFreeMemory;

/**
 * @author mdogan 03/12/13
 */
public final class StandardMemoryManager implements MemoryManager {

    public static final String PROPERTY_DEBUG_ENABLED = "hazelcast.memory.manager.debug.enabled";
    private static final boolean DEBUG = Boolean.getBoolean(PROPERTY_DEBUG_ENABLED);

    private final LibMalloc malloc;
    private final NativeMemoryStats memoryStats;

    private final Long2LongHashMap allocatedBlocks;

    public StandardMemoryManager(MemorySize cap) {
        long size = cap.bytes();
        checkFreeMemory(size);
        malloc = new UnsafeMalloc();
        memoryStats = new NativeMemoryStats(size);

        allocatedBlocks = initAllocatedBlocks();
    }

    StandardMemoryManager(LibMalloc malloc, NativeMemoryStats memoryStats) {
        this.malloc = malloc;
        this.memoryStats = memoryStats;

        allocatedBlocks = initAllocatedBlocks();
    }

    private static Long2LongHashMap initAllocatedBlocks() {
        if (DEBUG) {
            return new Long2LongHashMap(NULL_ADDRESS);
        }
        return null;
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

        if (address == NULL_ADDRESS) {
            throw new NativeOutOfMemoryError("Not enough contiguous memory available! " +
                    "Cannot acquire " + MemorySize.toPrettyString(size) + "!");
        }

        if (DEBUG) {
            traceAllocation(address, size);
        }

        UnsafeHelper.UNSAFE.setMemory(address, size, (byte) 0);
        memoryStats.addCommittedNative(size);
        return address;
    }

    private synchronized void traceAllocation(long address, long size) {
        long current = allocatedBlocks.put(address, size);
        if (current != NULL_ADDRESS) {
            throw new AssertionError("Already allocated! " + address);
        }
    }

    @Override
    public final void free(long address, long size) {
        assert address != NULL_ADDRESS : "Invalid address: " + address + ", size: " + size;
        assert size > 0 : "Invalid memory size: " + size + ", address: " + address;


        if (DEBUG) {
            traceRelease(address, size);
        }

        malloc.free(address);
        memoryStats.addCommittedNative(-size);
    }

    private synchronized void traceRelease(long address, long size) {
        long current = allocatedBlocks.remove(address);
        if (current != size) {
            if (current == NULL_ADDRESS) {
                throw new AssertionError("Either not allocated or duplicate free()! "
                        + "Address: " + address + ", Size: " + size);
            }
            throw new AssertionError("Invalid size! Address: " + address
                    + ", Expected: " + current + ", Actual: " + size);
        }
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
        if (DEBUG) {
            allocatedBlocks.clear();
        }
    }

    @Override
    public int getSize(long address) {
        return SIZE_INVALID;
    }

    public synchronized void forEachAllocatedBlock(LongLongConsumer consumer) {
        if (DEBUG) {
            allocatedBlocks.longForEach(consumer);
            return;
        }
        throw new UnsupportedOperationException("Allocated blocks are tracked only in DEBUG mode!");
    }
}
