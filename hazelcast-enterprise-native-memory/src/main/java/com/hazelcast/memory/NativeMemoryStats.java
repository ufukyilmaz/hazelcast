package com.hazelcast.memory;

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
    public final long getMax() {
        return maxNative;
    }

    @Override
    public final long getCommitted() {
        return committedNative.get();
    }

    @Override
    public long getUsed() {
        return getCommitted();
    }

    @Override
    public final long getFree() {
        long free = maxNative - getUsed();
        return (free > 0 ? free : 0L);
    }

    void checkAndAddCommittedNative(long size) {
        if (size > 0) {
            for (; ; ) {
                long currentAllocated = committedNative.get();
                long memoryAfterAllocation = currentAllocated + size;
                if (maxNative < memoryAfterAllocation) {
                    throw new NativeOutOfMemoryError("Not enough contiguous memory available!"
                            + " Cannot allocate " + MemorySize.toPrettyString(size)
                            + ". Max Native Memory: " + MemorySize.toPrettyString(maxNative)
                            + ", Committed Native Memory: " + MemorySize.toPrettyString(currentAllocated)
                            + ", Used Native Memory: " + MemorySize.toPrettyString(getUsed())
                    );
                }
                if (committedNative.compareAndSet(currentAllocated, memoryAfterAllocation)) {
                    break;
                }
            }
        }
    }

    final void addCommittedNative(long size) {
        committedNative.addAndGet(size);
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
        sb.append("NativeMemoryStats {");

        sb.append(", Max ").append(MemorySize.toPrettyString(getMax()));
        sb.append(", Committed ").append(MemorySize.toPrettyString(getCommitted()));
        sb.append(", Used ").append(MemorySize.toPrettyString(getUsed()));
        sb.append(", Free ").append(MemorySize.toPrettyString(getFree()));
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
