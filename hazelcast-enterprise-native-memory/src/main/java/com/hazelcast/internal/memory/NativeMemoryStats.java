package com.hazelcast.internal.memory;

import com.hazelcast.memory.MemorySize;

import java.util.concurrent.atomic.AtomicLong;

class NativeMemoryStats extends DefaultMemoryStats {

    private static final boolean ASSERTS_ENABLED;

    static {
        ASSERTS_ENABLED = NativeMemoryStats.class.desiredAssertionStatus();
    }

    protected final AtomicLong committedNative = new AtomicLong();

    private final long configuredMaxNative;
    private final AtomicLong maxNative;
    private final AtomicLong internalFragmentation = new AtomicLong();
    private final MemoryAdjuster memoryAdjuster;

    NativeMemoryStats(long maxNative) {
        this.maxNative = new AtomicLong(maxNative);
        this.configuredMaxNative = maxNative;
        this.memoryAdjuster = new MemoryAdjuster(this);
    }

    public MemoryAdjuster getMemoryAdjuster() {
        return memoryAdjuster;
    }

    @Override
    public final long getMaxNative() {
        return maxNative.get();
    }

    public boolean casMaxNative(long currentAllocated, long memoryAfterAllocation) {
        return maxNative.compareAndSet(currentAllocated, memoryAfterAllocation);
    }

    public long getConfiguredMaxNative() {
        return configuredMaxNative;
    }

    @Override
    public final long getCommittedNative() {
        return committedNative.get();
    }

    public boolean casCommittedNative(long currentAllocated, long memoryAfterAllocation) {
        return committedNative.compareAndSet(currentAllocated, memoryAfterAllocation);
    }

    @Override
    public long getUsedNative() {
        return getCommittedNative();
    }

    @Override
    public final long getFreeNative() {
        long free = getMaxNative() - getUsedNative();
        return (free > 0 ? free : 0L);
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
