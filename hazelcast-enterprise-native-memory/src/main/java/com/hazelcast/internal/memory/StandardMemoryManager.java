package com.hazelcast.internal.memory;

import com.hazelcast.internal.memory.impl.LibMalloc;
import com.hazelcast.internal.memory.impl.LibMallocFactory;
import com.hazelcast.internal.memory.impl.UnsafeMallocFactory;
import com.hazelcast.internal.metrics.StaticMetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.collection.Long2LongHashMap;
import com.hazelcast.internal.util.collection.Long2ObjectHashMap;
import com.hazelcast.internal.util.function.LongLongConsumer;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.NativeOutOfMemoryError;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;

public final class StandardMemoryManager implements HazelcastMemoryManager, StaticMetricsProvider {

    /**
     * System property to enable the debug mode of {@link StandardMemoryManager}.
     */
    public static final String PROPERTY_DEBUG_ENABLED = "hazelcast.memory.manager.debug.enabled";

    /**
     * System property to enable the debug mode with stacktraces of {@link StandardMemoryManager}.
     */
    public static final String PROPERTY_DEBUG_STACKTRACE_ENABLED = "hazelcast.memory.manager.debug.stacktrace.enabled";

    /**
     * Defines how many stacktrace elements are skipped, to hide internal method calls from user.
     */
    private static final int STACK_TRACE_OFFSET = 3;

    private static final FreeMemoryChecker DEFAULT_FREE_MEMORY_CHECKER = new FreeMemoryChecker();
    private static final LibMallocFactory DEFAULT_LIB_MALLOC_FACTORY = new UnsafeMallocFactory(DEFAULT_FREE_MEMORY_CHECKER);

    private final Counter sequenceGenerator = MwCounter.newMwCounter();

    private final boolean isDebugStackTraceEnabled;
    private final boolean isDebugEnabled;

    private final LibMalloc malloc;
    private final NativeMemoryStats memoryStats;

    private final Long2LongHashMap allocatedBlocks;
    private final Long2ObjectHashMap<String> allocatedStackTraces;
    private final StringBuilder stackTraceStringBuilder;

    public StandardMemoryManager(MemorySize cap) {
        this(cap, DEFAULT_LIB_MALLOC_FACTORY);
    }

    public StandardMemoryManager(MemorySize cap, LibMallocFactory libMallocFactory) {
        this.isDebugStackTraceEnabled = Boolean.getBoolean(PROPERTY_DEBUG_STACKTRACE_ENABLED);
        this.isDebugEnabled = isDebugStackTraceEnabled || Boolean.getBoolean(PROPERTY_DEBUG_ENABLED);

        long size = cap.bytes();

        this.malloc = libMallocFactory.create(size);
        this.memoryStats = new NativeMemoryStats(size);

        this.allocatedBlocks = initAllocatedBlocks();
        this.allocatedStackTraces = initAllocatedStacktraces();
        this.stackTraceStringBuilder = initStackTraceStringBuilder();
    }

    StandardMemoryManager(LibMalloc malloc, NativeMemoryStats memoryStats) {
        this.isDebugStackTraceEnabled = Boolean.getBoolean(PROPERTY_DEBUG_STACKTRACE_ENABLED);
        this.isDebugEnabled = isDebugStackTraceEnabled || Boolean.getBoolean(PROPERTY_DEBUG_ENABLED);

        this.malloc = malloc;
        this.memoryStats = memoryStats;

        this.allocatedBlocks = initAllocatedBlocks();
        this.allocatedStackTraces = initAllocatedStacktraces();
        this.stackTraceStringBuilder = initStackTraceStringBuilder();
    }

    private Long2LongHashMap initAllocatedBlocks() {
        if (isDebugEnabled) {
            return new Long2LongHashMap(NULL_ADDRESS);
        }
        return null;
    }

    private Long2ObjectHashMap<String> initAllocatedStacktraces() {
        if (isDebugStackTraceEnabled) {
            return new Long2ObjectHashMap<String>();
        }
        return null;
    }

    private StringBuilder initStackTraceStringBuilder() {
        if (isDebugStackTraceEnabled) {
            return new StringBuilder();
        }
        return null;
    }

    @Override
    public MemoryStats getMemoryStats() {
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

            AMEM.setMemory(address, size, (byte) 0);

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
                AMEM.setMemory(startAddress, diff, (byte) 0);
            }

            return newAddress;
        } catch (Throwable t) {
            if (diff > 0) {
                memoryStats.removeCommittedNative(diff);
            }
            throw ExceptionUtil.rethrow(t);
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

    @Override
    public void compact() {
    }

    @Override
    public MemoryAllocator getSystemAllocator() {
        return this;
    }

    @Override
    public boolean isDisposed() {
        return false;
    }

    @Override
    public void dispose() {
        if (isDebugEnabled) {
            allocatedBlocks.clear();
        }
        malloc.dispose();
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

    @Override
    public long newSequence() {
        return sequenceGenerator.inc();
    }

    @Override
    public void provideStaticMetrics(MetricsRegistry registry) {
        registry.registerStaticMetrics(memoryStats, "memorymanager.stats");
    }

    public synchronized void forEachAllocatedBlock(LongLongConsumer consumer) {
        if (!isDebugEnabled) {
            throw new UnsupportedOperationException("Allocated blocks are tracked only in DEBUG mode!");
        }
        allocatedBlocks.longForEach(consumer);
    }

    public synchronized Long2ObjectHashMap<String> getAllocatedStackTraces() {
        if (!isDebugStackTraceEnabled) {
            throw new UnsupportedOperationException("Allocated blocks are tracked only in DEBUG_STACKTRACE mode!");
        }
        return allocatedStackTraces;
    }

    private synchronized void traceAllocation(long address, long size) {
        long current = allocatedBlocks.put(address, size);
        if (current != NULL_ADDRESS) {
            throw new AssertionError("Already allocated! " + address);
        }
        if (isDebugStackTraceEnabled) {
            allocatedStackTraces.put(address, getStackTrace());
        }
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
        if (isDebugStackTraceEnabled) {
            allocatedStackTraces.remove(address);
        }
    }

    private String getStackTrace() {
        String prefix = "";
        StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        for (int i = STACK_TRACE_OFFSET; i < stackTraceElements.length; i++) {
            stackTraceStringBuilder.append(prefix).append(stackTraceElements[i].toString());
            prefix = "\n\t";
        }

        String stackTrace = stackTraceStringBuilder.toString();
        stackTraceStringBuilder.setLength(0);
        return stackTrace;
    }

    protected static void checkNotNull(long address, long size) {
        if (address == NULL_ADDRESS) {
            throw new NativeOutOfMemoryError("Not enough contiguous memory available!"
                    + " Cannot acquire " + MemorySize.toPrettyString(size) + "!");
        }
    }
}
