package com.hazelcast.internal.memory;

import com.hazelcast.memory.MemorySize;

import java.util.concurrent.atomic.AtomicLong;

public class PooledNativeMemoryStats extends NativeMemoryStats implements MemoryStats {

    private final AtomicLong usedNative = new AtomicLong();
    private final AtomicLong usedMetadata = new AtomicLong();

    private final long maxMetadata;

    public PooledNativeMemoryStats(long maxNative, long maxMetadata) {
        super(maxNative);
        this.maxMetadata = maxMetadata;
    }

    @Override
    public final long getMaxMetadata() {
        return maxMetadata;
    }

    @Override
    public long getUsedNative() {
        return usedNative.get();
    }

    final void addUsedNativeMemory(long size) {
        usedNative.addAndGet(size);
    }

    final void removeUsedNativeMemory(long size) {
        usedNative.addAndGet(-size);
    }

    final void addMetadataUsage(long size) {
        usedMetadata.addAndGet(size);
    }

    final void removeMetadataUsage(long size) {
        usedMetadata.addAndGet(-size);
    }

    @Override
    public final long getUsedMetadata() {
        return usedMetadata.get();
    }

    @Override
    void appendAdditionalToString(StringBuilder sb) {
        sb.append(", Max Metadata: ").append(MemorySize.toPrettyString(maxMetadata));
        sb.append(", Used Metadata: ").append(MemorySize.toPrettyString(usedMetadata.get()));
    }

    void resetUsedNativeMemory() {
        usedNative.set(0L);
    }
}
