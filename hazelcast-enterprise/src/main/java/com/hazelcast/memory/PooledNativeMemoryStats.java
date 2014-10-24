package com.hazelcast.memory;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.monitor.LocalMemoryStats;
import com.hazelcast.util.Clock;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mdogan 10/02/14
 */
class PooledNativeMemoryStats extends NativeMemoryStats implements LocalMemoryStats {

    private final long creationTime;

    private final long maxMetadata;

    private final AtomicLong usedMetadata = new AtomicLong();

    private final AtomicLong usedNative = new AtomicLong();

    public PooledNativeMemoryStats(long maxNative, long maxMetadata) {
        super(maxNative);
        this.maxMetadata = maxMetadata;
        creationTime = Clock.currentTimeMillis();
    }

    final long getMaxMetadata() {
        return maxMetadata;
    }

    @Override
    public long getUsedNativeMemory() {
        return usedNative.get();
    }

    final void addUsedOffHeap(long size) {
        usedNative.addAndGet(size);
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
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("creationTime", getCreationTime());
        root.add("totalPhysical", getTotalPhysical());
        root.add("freePhysical", getFreePhysical());
        root.add("maxNativeMemory", getMaxNativeMemory() + maxMetadata);
        root.add("committedNativeMemory", getCommittedNativeMemory() + maxMetadata);
        root.add("usedNativeMemory", getUsedNativeMemory() + maxMetadata);
        root.add("freeNativeMemory", getFreeNativeMemory());
        root.add("maxHeap", getMaxHeap());
        root.add("committedHeap", getCommittedHeap());
        root.add("usedHeap", getUsedHeap());
        root.add("gcStats", getGCStats().toJson());
        return root;
    }
}
