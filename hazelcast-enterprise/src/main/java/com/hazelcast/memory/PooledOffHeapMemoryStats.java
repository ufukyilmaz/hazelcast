package com.hazelcast.memory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mdogan 10/02/14
 */
class PooledOffHeapMemoryStats extends OffHeapMemoryStats implements MemoryStats {

    private final long maxMetadata;

    private final AtomicLong usedMetadata = new AtomicLong();

    private final AtomicLong usedOffHeap = new AtomicLong();

    public PooledOffHeapMemoryStats(long maxOffHeap, long maxMetadata) {
        super(maxOffHeap);
        this.maxMetadata = maxMetadata;
    }

    final long getMaxMetadata() {
        return maxMetadata;
    }

    public long getUsedOffHeap() {
        return usedOffHeap.get();
    }

    final void addUsedOffHeap(long size) {
        usedOffHeap.addAndGet(size);
    }

    final void addMetadataUsage(long size) {
        usedMetadata.addAndGet(size);
    }

    final long getUsedMetadata() {
        return usedMetadata.get();
    }

    @Override
    void appendAdditionalToString(StringBuilder sb) {
        sb.append(", Max Metadata: ").append(MemorySize.toPrettyString(maxMetadata));
        sb.append(", Used Metadata: ").append(MemorySize.toPrettyString(usedMetadata.get()));
    }

    @Override
    public SerializableMemoryStats asSerializable() {
        SerializableMemoryStats stats = super.asSerializable();
        stats.setMaxOffHeap(getMaxOffHeap() + maxMetadata);
        stats.setCommittedOffHeap(getCommittedOffHeap() + maxMetadata);
        stats.setUsedOffHeap(getUsedOffHeap() + maxMetadata);
        return stats;
    }

}
