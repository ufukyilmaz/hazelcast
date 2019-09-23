package com.hazelcast.internal.memory;

import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.NativeOutOfMemoryError;

import java.util.concurrent.atomic.AtomicLong;

class NativeMemoryStats extends DefaultMemoryStats {

    private static final boolean ASSERTS_ENABLED;

    static {
        ASSERTS_ENABLED = NativeMemoryStats.class.desiredAssertionStatus();
    }

    private final long maxNative;

    private final AtomicLong committedNative = new AtomicLong();

    private final AtomicLong internalFragmentation = new AtomicLong();

    NativeMemoryStats(long maxNative) {
        this.maxNative = maxNative;
    }

    @Override
    public final long getMaxNative() {
        return maxNative;
    }

    @Override
    public final long getCommittedNative() {
        return committedNative.get();
    }

    @Override
    public long getUsedNative() {
        return getCommittedNative();
    }

    @Override
    public final long getFreeNative() {
        long free = maxNative - getUsedNative();
        return (free > 0 ? free : 0L);
    }

    void checkAndAddCommittedNative(long size) {
        if (size > 0) {
            for (; ; ) {
                long currentAllocated = committedNative.get();
                long memoryAfterAllocation = currentAllocated + size;
                if (maxNative < memoryAfterAllocation) {
                    throw new NativeOutOfMemoryError("Not enough contiguous memory available!"
                            + " Cannot allocate " + MemorySize.toPrettyString(size) + "!"
                            + " Max Native Memory: " + MemorySize.toPrettyString(maxNative)
                            + ", Committed Native Memory: " + MemorySize.toPrettyString(currentAllocated)
                            + ", Used Native Memory: " + MemorySize.toPrettyString(getUsedNative())
                    );
                }
                if (committedNative.compareAndSet(currentAllocated, memoryAfterAllocation)) {
                    break;
                }
            }
        }
    }

    final void removeCommittedNative(long size) {
        committedNative.addAndGet(-size);
    }

    final void addInternalFragmentation(long size) {
        if (ASSERTS_ENABLED) {
            internalFragmentation.addAndGet(size);
        }
    }

    final void removeInternalFragmentation(long size) {
        if (ASSERTS_ENABLED) {
            internalFragmentation.addAndGet(-size);
        }
    }

    @Override
    public final String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("NativeMemoryStats{");
        sb.append("Total Physical: ").append(MemorySize.toPrettyString(getTotalPhysical()));
        sb.append(", Free Physical: ").append(MemorySize.toPrettyString(getFreePhysical()));
        sb.append(", Max Heap: ").append(MemorySize.toPrettyString(getMaxHeap()));
        sb.append(", Committed Heap: ").append(MemorySize.toPrettyString(getCommittedHeap()));
        sb.append(", Used Heap: ").append(MemorySize.toPrettyString(getUsedHeap()));
        sb.append(", Free Heap: ").append(MemorySize.toPrettyString(getFreeHeap()));
        sb.append(", Max Native Memory: ").append(MemorySize.toPrettyString(getMaxNative()));
        sb.append(", Committed Native Memory: ").append(MemorySize.toPrettyString(getCommittedNative()));
        sb.append(", Used Native Memory: ").append(MemorySize.toPrettyString(getUsedNative()));
        sb.append(", Free Native Memory: ").append(MemorySize.toPrettyString(getFreeNative()));
        appendAdditionalToString(sb);
        if (ASSERTS_ENABLED) {
            sb.append(", Internal Fragmentation: ").append(MemorySize.toPrettyString(internalFragmentation.get()));
        }
        sb.append(", GC Stats: ");
        sb.append(getGCStats());
        sb.append('}');
        return sb.toString();
    }

    void appendAdditionalToString(StringBuilder sb) {
    }
}
