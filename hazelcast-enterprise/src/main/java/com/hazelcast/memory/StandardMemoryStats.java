package com.hazelcast.memory;

import static com.hazelcast.memory.MemoryStatsSupport.freePhysicalMemory;
import static com.hazelcast.memory.MemoryStatsSupport.getHeapMemoryUsage;
import static com.hazelcast.memory.MemoryStatsSupport.totalPhysicalMemory;

class StandardMemoryStats implements MemoryStats {

    private final StandardGCStats gcStats = new StandardGCStats();

    public final long getTotalPhysical() {
        return totalPhysicalMemory();
    }

    @Override
    public final long getFreePhysical() {
        return freePhysicalMemory();
    }

    @Override
    public final long getMaxHeap() {
        return getHeapMemoryUsage().getMax();
    }

    @Override
    public final long getCommittedHeap() {
        return getHeapMemoryUsage().getCommitted();
    }

    @Override
    public final long getUsedHeap() {
        return getHeapMemoryUsage().getUsed();
    }

    @Override
    public final long getFreeHeap() {
        return getMaxHeap() - getUsedHeap();
    }

    @Override
    public long getMaxOffHeap() {
        return 0;
    }

    @Override
    public long getCommittedOffHeap() {
        return 0;
    }

    @Override
    public long getUsedOffHeap() {
        return 0;
    }

    @Override
    public long getFreeOffHeap() {
        return 0L;
    }

    @Override
    public final GCStats getGCStats() {
        gcStats.refresh();
        return gcStats;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(":: MEMORY STATS :: ").append('\n');

        sb.append("Total Physical: ").append(MemorySize.toPrettyString(getTotalPhysical()));
        sb.append(", Free Physical: ").append(MemorySize.toPrettyString(getFreePhysical()));
        sb.append(", Max Heap: ").append(MemorySize.toPrettyString(getMaxHeap()));
        sb.append(", Committed Heap: ").append(MemorySize.toPrettyString(getCommittedHeap()));
        sb.append(", Used Heap: ").append(MemorySize.toPrettyString(getUsedHeap()));
        sb.append(", Free Heap: ").append(MemorySize.toPrettyString(getFreeHeap()));
        sb.append('\n');
        sb.append(getGCStats());
        return sb.toString();
    }

    @Override
    public SerializableMemoryStats asSerializable() {
        SerializableMemoryStats stats = new SerializableMemoryStats();
        stats.setTotalPhysical(getTotalPhysical());
        stats.setFreePhysical(getFreePhysical());
        stats.setMaxOffHeap(getMaxOffHeap());
        stats.setCommittedOffHeap(getCommittedOffHeap());
        stats.setUsedOffHeap(getUsedOffHeap());
        stats.setMaxHeap(getMaxHeap());
        stats.setCommittedHeap(getCommittedHeap());
        stats.setUsedHeap(getUsedHeap());
        stats.setGcStats(gcStats.asSerializable());
        return stats;
    }
}
