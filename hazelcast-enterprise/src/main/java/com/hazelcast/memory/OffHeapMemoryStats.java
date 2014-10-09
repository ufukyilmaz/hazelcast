package com.hazelcast.memory;

import com.hazelcast.memory.error.OffHeapOutOfMemoryError;

import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.memory.MemoryStatsSupport.checkFreeMemory;

/**
 * @author mdogan 10/02/14
 */
class OffHeapMemoryStats extends StandardMemoryStats implements MemoryStats {

    private static final boolean ASSERTS_ENABLED;

    static {
        ASSERTS_ENABLED = OffHeapMemoryStats.class.desiredAssertionStatus();
    }

    private final long maxOffHeap;

    private final AtomicLong committedOffHeap = new AtomicLong();

    private final AtomicLong internalFragmentation = new AtomicLong();

    OffHeapMemoryStats(long maxOffHeap) {
        this.maxOffHeap = maxOffHeap;
    }

    @Override
    public final long getMaxOffHeap() {
        return maxOffHeap;
    }

    @Override
    public final long getCommittedOffHeap() {
        return committedOffHeap.get();
    }

    @Override
    public long getUsedOffHeap() {
        return getCommittedOffHeap();
    }

    @Override
    public final long getFreeOffHeap() {
        long free = maxOffHeap - getUsedOffHeap();
        return free > 0 ? free : 0L;
    }

    final void checkOffHeapAllocation(long size) {
        checkCommittedOffHeap(size);
        checkFreeMemory(size);
    }

    private void checkCommittedOffHeap(long size) {
        if (size > 0) {
            long currentAllocated = committedOffHeap.get();
            if (maxOffHeap < (currentAllocated + size)) {
                throw new OffHeapOutOfMemoryError("Not enough contiguous memory available! " +
                        " Cannot allocate " + MemorySize.toPrettyString(size) + "!" +
                        " Max OffHeap: " + MemorySize.toPrettyString(maxOffHeap) +
                        ", Committed OffHeap: " + MemorySize.toPrettyString(currentAllocated) +
                        ", Used OffHeap: " + MemorySize.toPrettyString(getUsedOffHeap())
                );
            }
        }
    }

    final void addCommittedOffHeap(long size) {
        committedOffHeap.addAndGet(size);
    }

    final void addInternalFragmentation(long size) {
        if (ASSERTS_ENABLED) {
            internalFragmentation.addAndGet(size);
        }
    }

    @Override
    public final String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(":: MEMORY STATS :: ").append('\n');

        sb.append("Total Physical: ").append(MemorySize.toPrettyString(getTotalPhysical()));
        sb.append(", Free Physical: ").append(MemorySize.toPrettyString(getFreePhysical()));
        sb.append(", Max Heap: ").append(MemorySize.toPrettyString(getMaxHeap()));
        sb.append(", Committed Heap: ").append(MemorySize.toPrettyString(getCommittedHeap()));
        sb.append(", Used Heap: ").append(MemorySize.toPrettyString(getUsedHeap()));
        sb.append(", Free Heap: ").append(MemorySize.toPrettyString(getFreeHeap()));
        sb.append(", Max OffHeap: ").append(MemorySize.toPrettyString(getMaxOffHeap()));
        sb.append(", Committed OffHeap: ").append(MemorySize.toPrettyString(getCommittedOffHeap()));
        sb.append(", Used OffHeap: ").append(MemorySize.toPrettyString(getUsedOffHeap()));
        sb.append(", Free OffHeap: ").append(MemorySize.toPrettyString(getFreeOffHeap()));
        appendAdditionalToString(sb);
        if (ASSERTS_ENABLED) {
            sb.append(", Internal Fragmentation: ").append(MemorySize.toPrettyString(internalFragmentation.get()));
        }
        sb.append('\n');
        sb.append(getGCStats());
        return sb.toString();
    }

    void appendAdditionalToString(StringBuilder sb) {
    }
}
