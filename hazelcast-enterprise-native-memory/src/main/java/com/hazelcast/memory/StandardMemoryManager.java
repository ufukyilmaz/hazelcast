package com.hazelcast.memory;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry;
import com.hazelcast.internal.memory.GlobalMemoryAccessorType;
import com.hazelcast.internal.memory.impl.LibMalloc;
import com.hazelcast.internal.memory.impl.UnsafeMalloc;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.collection.Long2LongHashMap;
import com.hazelcast.util.function.LongLongConsumer;

import static com.hazelcast.memory.FreeMemoryChecker.checkFreeMemory;

public final class StandardMemoryManager implements MemoryManager {

    /**
     * System property to enable debug mode of {@link StandardMemoryManager}.
     */
    public static final String PROPERTY_DEBUG_ENABLED = "hazelcast.memory.manager.debug.enabled";

    // We are using `STANDARD` memory accessor because we internally guarantee that
    // every memory access is aligned.
    private static final MemoryAccessor MEMORY_ACCESSOR =
            GlobalMemoryAccessorRegistry.getGlobalMemoryAccessor(GlobalMemoryAccessorType.STANDARD);

    private final boolean isDebugEnabled;

    private final LibMalloc malloc;
    private final NativeMemoryStats memoryStats;
    private final Counter sequenceGenerator = MwCounter.newMwCounter();

    private final Long2LongHashMap allocatedBlocks;

    public StandardMemoryManager(MemorySize cap) {
        isDebugEnabled = Boolean.getBoolean(PROPERTY_DEBUG_ENABLED);

        long size = cap.bytes();
        checkFreeMemory(size);
        malloc = new UnsafeMalloc();
        memoryStats = new NativeMemoryStats(size);

        allocatedBlocks = initAllocatedBlocks();
    }

    StandardMemoryManager(LibMalloc malloc, NativeMemoryStats memoryStats) {
        isDebugEnabled = Boolean.getBoolean(PROPERTY_DEBUG_ENABLED);

        this.malloc = malloc;
        this.memoryStats = memoryStats;

        allocatedBlocks = initAllocatedBlocks();
    }

    private Long2LongHashMap initAllocatedBlocks() {
        if (isDebugEnabled) {
            return new Long2LongHashMap(NULL_ADDRESS);
        }
        return null;
    }

    @Override
    public JVMMemoryStats getMemoryStats() {
        return memoryStats;
    }

    @Override
    public long allocate(long size) {
        assert size > 0 : "Size must be positive: " + size;

        memoryStats.checkAndAddCommittedNative(size);

        try {
            long address = malloc.malloc(size);
            checkNotNull(address, size);

            if (isDebugEnabled) {
                traceAllocation(address, size);
            }

            MEMORY_ACCESSOR.setMemory(address, size, (byte) 0);

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

            if (isDebugEnabled) {
                traceRelease(address, currentSize);
                traceAllocation(newAddress, newSize);
            }

            if (diff > 0) {
                long startAddress = newAddress + currentSize;
                MEMORY_ACCESSOR.setMemory(startAddress, diff, (byte) 0);
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
            throw new NativeOutOfMemoryError("Not enough contiguous memory available!"
                    + " Cannot acquire " + MemorySize.toPrettyString(size) + "!");
        }
    }

    private synchronized void traceAllocation(long address, long size) {
        long current = allocatedBlocks.put(address, size);
        if (current != NULL_ADDRESS) {
            throw new AssertionError("Already allocated! " + address);
        }
    }

    @Override
    public void free(long address, long size) {
        assert address != NULL_ADDRESS : "Invalid address: " + address + ", size: " + size;
        assert size > 0 : "Invalid memory size: " + size + ", address: " + address;

        if (isDebugEnabled) {
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
        if (isDebugEnabled) {
            allocatedBlocks.clear();
        }
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
        if (isDebugEnabled) {
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
