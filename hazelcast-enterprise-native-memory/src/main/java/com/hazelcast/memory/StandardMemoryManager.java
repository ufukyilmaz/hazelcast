package com.hazelcast.memory;

import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.collection.Long2LongHashMap;
import com.hazelcast.util.counters.Counter;
import com.hazelcast.util.counters.MwCounter;
import com.hazelcast.util.function.LongLongConsumer;

import static com.hazelcast.memory.FreeMemoryChecker.checkFreeMemory;

/**
 * @author mdogan 03/12/13
 */
public final class StandardMemoryManager implements MemoryManager {

    public static final String PROPERTY_DEBUG_ENABLED = "hazelcast.memory.manager.debug.enabled";

    private final boolean DEBUG;

    private final LibMalloc malloc;
    private final NativeMemoryStats memoryStats;
    private final Counter sequenceGenerator = MwCounter.newMwCounter();

    private final Long2LongHashMap allocatedBlocks;

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

    private Long2LongHashMap initAllocatedBlocks() {
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

        memoryStats.checkAndAddCommittedNative(size);

        try {
            long address = malloc.malloc(size);
            checkNotNull(address, size);

            if (DEBUG) {
                traceAllocation(address, size);
            }

            UnsafeHelper.UNSAFE.setMemory(address, size, (byte) 0);

            return address;
        } catch (Throwable t) {
            memoryStats.removeCommittedNative(size);
            throw ExceptionUtil.rethrow(t);
        }
    }

    @Override
    public long reallocate(long address, long currentSize, long newSize) {
        assert currentSize > 0 : "Current size must be positive: " + currentSize;
        assert newSize > 0 : "New size must be positive: " + newSize;

        long diff = newSize - currentSize;
        if (diff > 0) {
            memoryStats.checkAndAddCommittedNative(diff);
        }

        try {
            long newAddress = malloc.realloc(address, newSize);
            checkNotNull(newAddress, newSize);

            if (DEBUG) {
                traceRelease(address, currentSize);
                traceAllocation(newAddress, newSize);
            }

            if (diff > 0) {
                long startAddress = newAddress + currentSize;
                UnsafeHelper.UNSAFE.setMemory(startAddress, diff, (byte) 0);
            }

            return newAddress;
        } catch (Throwable t) {
            if (diff > 0) {
                memoryStats.removeCommittedNative(diff);
            }
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected static void checkNotNull(long address, long size) {
        if (address == NULL_ADDRESS) {
            throw new NativeOutOfMemoryError("Not enough contiguous memory available! " +
                    "Cannot acquire " + MemorySize.toPrettyString(size) + "!");
        }
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
        memoryStats.removeCommittedNative(size);
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
        memoryStats.reset();
    }

    @Override
    public long getUsableSize(long address) {
        return SIZE_INVALID;
    }

    @Override
    public long validateAndGetUsableSize(long address) {
        return SIZE_INVALID;
    }

    @Override
    public long getAllocatedSize(long address) {
        return SIZE_INVALID;
    }

    @Override
    public long validateAndGetAllocatedSize(long address) {
        return SIZE_INVALID;
    }

    public synchronized void forEachAllocatedBlock(LongLongConsumer consumer) {
        if (DEBUG) {
            allocatedBlocks.longForEach(consumer);
            return;
        }
        throw new UnsupportedOperationException("Allocated blocks are tracked only in DEBUG mode!");
    }

    @Override
    public long newSequence() {
        return sequenceGenerator.inc();
    }
}
