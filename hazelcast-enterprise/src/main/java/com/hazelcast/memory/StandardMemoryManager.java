package com.hazelcast.memory;

import com.hazelcast.nio.UnsafeHelper;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.memory.FreeMemoryChecker.checkFreeMemory;

/**
 * @author mdogan 03/12/13
 */
public final class StandardMemoryManager implements MemoryManager {

    public static final String PROPERTY_DEBUG_ENABLED = "hazelcast.memory.manager.debug.enabled";

    // Defined as non-static so a test can set this flag and after its job finished,
    // it can clear this flag so it doesn't effect others.
    private final boolean DEBUG;

    private final LibMalloc malloc;
    private final NativeMemoryStats memoryStats;

    private final Map<Long, Long> allocatedBlocks;

    public StandardMemoryManager(MemorySize cap) {
        DEBUG = Boolean.getBoolean(PROPERTY_DEBUG_ENABLED);

        long size = cap.bytes();
        checkFreeMemory(size);
        malloc = new UnsafeMalloc();
        memoryStats = new NativeMemoryStats(size);

        allocatedBlocks = initAllocatedBlocks();
    }

    StandardMemoryManager(LibMalloc malloc, NativeMemoryStats memoryStats) {
        DEBUG = Boolean.getBoolean(PROPERTY_DEBUG_ENABLED);

        this.malloc = malloc;
        this.memoryStats = memoryStats;

        allocatedBlocks = initAllocatedBlocks();
    }

    private Map initAllocatedBlocks() {
        if (DEBUG) {
            return new HashMap<Long, Long>();
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
        Long current = allocatedBlocks.put(address, size);
        if (current != null) {
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
        Long current = allocatedBlocks.remove(address);
        if (current == null) {
            throw new AssertionError("Either not allocated or duplicate free()! "
                    + "Address: " + address + ", Size: " + size);
        } else if (current != size) {
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
    public int getSize(long address) {
        return SIZE_INVALID;
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
            clearAllocatedBlocks();
        }
    }

    private synchronized void clearAllocatedBlocks() {
        allocatedBlocks.clear();
    }

    public synchronized void forEachAllocatedBlock(AllocatedBlockIterator iterator) {
        if (DEBUG) {
            for (Map.Entry<Long, Long> entry : allocatedBlocks.entrySet()) {
                iterator.onAllocatedBlock(entry.getKey(), entry.getValue());
            }
            return;
        }
        throw new UnsupportedOperationException("Allocated blocks are tracked only in DEBUG mode!");
    }

    public interface AllocatedBlockIterator {

        void onAllocatedBlock(long address, long size);

    }

}
