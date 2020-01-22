package com.hazelcast.internal.hidensity;

import com.hazelcast.internal.metrics.Probe;

import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.HD_METRIC_ENTRY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.HD_METRIC_FORCE_EVICTED_ENTRY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.HD_METRIC_FORCE_EVICTION_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.HD_METRIC_USED_MEMORY;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.metrics.ProbeUnit.BYTES;

/**
 * Holds information about Hi-Density storage such as entry count, used memory, etc.
 */
public class HiDensityStorageInfo {

    private final String storageName;
    @Probe(name = HD_METRIC_USED_MEMORY, level = MANDATORY, unit = BYTES)
    private final AtomicLong usedMemory = new AtomicLong(0L);
    @Probe(name = HD_METRIC_FORCE_EVICTION_COUNT, level = MANDATORY)
    private final AtomicLong forceEvictionCount = new AtomicLong(0L);
    @Probe(name = HD_METRIC_FORCE_EVICTED_ENTRY_COUNT, level = MANDATORY)
    private final AtomicLong forceEvictedEntryCount = new AtomicLong(0L);
    @Probe(name = HD_METRIC_ENTRY_COUNT, level = MANDATORY)
    private final AtomicLong entryCount = new AtomicLong(0L);

    public HiDensityStorageInfo(String storageName) {
        this.storageName = storageName;
    }

    public String getStorageName() {
        return storageName;
    }

    public long addEntryCount(long count) {
        return entryCount.addAndGet(count);
    }

    public long removeEntryCount(long count) {
        return entryCount.addAndGet(-count);
    }

    public long increaseEntryCount() {
        return entryCount.incrementAndGet();
    }

    public long decreaseEntryCount() {
        return entryCount.decrementAndGet();
    }

    public long getEntryCount() {
        return entryCount.get();
    }

    public long addUsedMemory(long size) {
        return usedMemory.addAndGet(size);
    }

    public long removeUsedMemory(long size) {
        return usedMemory.addAndGet(-size);
    }

    public long getUsedMemory() {
        return usedMemory.get();
    }

    public long increaseForceEvictionCount() {
        return forceEvictionCount.incrementAndGet();
    }

    public long getForceEvictionCount() {
        return forceEvictionCount.get();
    }

    public long increaseForceEvictedEntryCount(long evictedEntryCount) {
        return forceEvictedEntryCount.addAndGet(evictedEntryCount);
    }

    public long getForceEvictedEntryCount() {
        return forceEvictedEntryCount.get();
    }

    @Override
    public int hashCode() {
        return storageName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof HiDensityStorageInfo)) {
            return false;
        }
        return storageName.equals(((HiDensityStorageInfo) obj).storageName);
    }

    @Override
    public String toString() {
        return "HiDensityStorageInfo{"
                + "storageName='" + storageName + '\''
                + ", usedMemory=" + usedMemory.get()
                + ", forceEvictionCount=" + forceEvictionCount.get()
                + ", forceEvictedEntryCount=" + forceEvictedEntryCount.get()
                + ", entryCount=" + entryCount.get()
                + '}';
    }
}
