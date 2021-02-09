package com.hazelcast.internal.memory;

import com.hazelcast.memory.MemorySize;

import java.util.concurrent.atomic.AtomicLong;

public class PooledNativeMemoryStats extends NativeMemoryStats implements MemoryStats {

    private final AtomicLong usedNative = new AtomicLong();
    private final AtomicLong usedMetadata = new AtomicLong();

    private final long maxMetadata;
    private final int pageSize;

    public PooledNativeMemoryStats(long maxNative, long maxMetadata, int pageSize) {
        super(maxNative);
        this.maxMetadata = maxMetadata;
        this.pageSize = pageSize;
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
        usedNative.addAndGet(size);
    }

    @SuppressWarnings("checkstyle:multiplevariabledeclarations")
    final void removeMetadataUsage(long size) {
        usedMetadata.addAndGet(-size);
        usedNative.addAndGet(-size);
        committedNative.addAndGet(-size);

        adjustMaxNative(size);
    }

    /**
     * Adjusts maxNative memory by decreasing it at
     * most given size. After decrease, maxNative
     * cannot be smaller than configuredMaxNative.
     *
     * @param size size to decrease
     */
    @SuppressWarnings("checkstyle:multiplevariabledeclarations")
    private void adjustMaxNative(long size) {
        long maxNative, configuredMaxNative, nextMaxNative;
        do {
            maxNative = getMaxNative();
            configuredMaxNative = getConfiguredMaxNative();
            if (maxNative <= configuredMaxNative) {
                break;
            }

            assert maxNative - size >= 0;

            nextMaxNative = maxNative - size;
            nextMaxNative = nextMaxNative < configuredMaxNative
                    ? configuredMaxNative : nextMaxNative;

        } while (!casMaxNative(maxNative, nextMaxNative));
    }

    public int getPageSize() {
        return pageSize;
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
