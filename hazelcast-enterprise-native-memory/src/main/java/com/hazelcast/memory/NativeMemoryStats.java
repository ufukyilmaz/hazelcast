package com.hazelcast.memory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mdogan 10/02/14
 */
public class NativeMemoryStats extends DefaultMemoryStats implements MemoryStats {

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
    public final long getMaxNativeMemory() {
        return maxNative;
    }

    @Override
    public final long getCommittedNativeMemory() {
        return committedNative.get();
    }

    @Override
    public long getUsedNativeMemory() {
        return getCommittedNativeMemory();
    }

    @Override
    public final long getFreeNativeMemory() {
        long free = maxNative - getUsedNativeMemory();
        return free > 0 ? free : 0L;
    }

    void checkCommittedNative(long size) {
        if (size > 0) {
            long currentAllocated = committedNative.get();
            if (maxNative < (currentAllocated + size)) {
                throw new NativeOutOfMemoryError("Not enough contiguous memory available! " +
                        " Cannot allocate " + MemorySize.toPrettyString(size) + "!" +
                        " Max Native Memory: " + MemorySize.toPrettyString(maxNative) +
                        ", Committed Native Memory: " + MemorySize.toPrettyString(currentAllocated) +
                        ", Used Native Memory: " + MemorySize.toPrettyString(getUsedNativeMemory())
                );
            }
        }
    }

    final void addCommittedNative(long size) {
        committedNative.addAndGet(size);
    }

    final void addInternalFragmentation(long size) {
        if (ASSERTS_ENABLED) {
            internalFragmentation.addAndGet(size);
        }
    }

    @Override
    public final String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("NativeMemoryStats {");

        sb.append("Total Physical: ").append(MemorySize.toPrettyString(getTotalPhysical()));
        sb.append(", Free Physical: ").append(MemorySize.toPrettyString(getFreePhysical()));
        sb.append(", Max Heap: ").append(MemorySize.toPrettyString(getMaxHeap()));
        sb.append(", Committed Heap: ").append(MemorySize.toPrettyString(getCommittedHeap()));
        sb.append(", Used Heap: ").append(MemorySize.toPrettyString(getUsedHeap()));
        sb.append(", Free Heap: ").append(MemorySize.toPrettyString(getFreeHeap()));
        sb.append(", Max Native Memory: ").append(MemorySize.toPrettyString(getMaxNativeMemory()));
        sb.append(", Committed Native Memory: ").append(MemorySize.toPrettyString(getCommittedNativeMemory()));
        sb.append(", Used Native Memory: ").append(MemorySize.toPrettyString(getUsedNativeMemory()));
        sb.append(", Free Native Memory: ").append(MemorySize.toPrettyString(getFreeNativeMemory()));
        appendAdditionalToString(sb);
        if (ASSERTS_ENABLED) {
            sb.append(", Internal Fragmentation: ").append(MemorySize.toPrettyString(internalFragmentation.get()));
        }
        sb.append(", ");
        sb.append(getGCStats());
        sb.append('}');
        return sb.toString();
    }

    void appendAdditionalToString(StringBuilder sb) {
    }
}
