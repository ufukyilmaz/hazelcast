package com.hazelcast.internal.hidensity;

import com.hazelcast.internal.metrics.Probe;

import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;

/**
 * Holds information about Hi-Density storage such as entry count, used memory, etc.
 */
public class HiDensityStorageInfo {

    private final String storageName;
    private final AtomicLong usedMemory = new AtomicLong(0L);
    private final AtomicLong forceEvictionCount = new AtomicLong(0L);
    private final AtomicLong forceEvictedEntryCount = new AtomicLong(0L);
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

    @Probe(name = "entryCount", level = MANDATORY)
    public long getEntryCount() {
        return entryCount.get();
    }

    public long addUsedMemory(long size) {
        return usedMemory.addAndGet(size);
    }

    public long removeUsedMemory(long size) {
        return usedMemory.addAndGet(-size);
    }

    @Probe(name = "usedMemory", level = MANDATORY)
    public long getUsedMemory() {
        return usedMemory.get();
    }

    public long increaseForceEvictionCount() {
        return forceEvictionCount.incrementAndGet();
    }

    @Probe(name = "forceEvictionCount", level = MANDATORY)
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
