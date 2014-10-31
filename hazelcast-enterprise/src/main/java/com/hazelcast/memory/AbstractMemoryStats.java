package com.hazelcast.memory;

import com.hazelcast.com.eclipsesource.json.JsonObject;
import com.hazelcast.memory.error.NativeMemoryOutOfMemoryError;
import com.hazelcast.monitor.LocalGCStats;
import com.hazelcast.monitor.LocalMemoryStats;
import com.hazelcast.monitor.impl.LocalGCStatsImpl;

import static com.hazelcast.memory.MemoryStatsSupport.freePhysicalMemory;
import static com.hazelcast.memory.MemoryStatsSupport.getHeapMemoryUsage;
import static com.hazelcast.memory.MemoryStatsSupport.totalPhysicalMemory;

abstract class AbstractMemoryStats implements LocalMemoryStats {

    private final LocalGCStatsImpl gcStats = new LocalGCStatsImpl();

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
    public final LocalGCStats getGCStats() {
        GCStatsSupport.fill(gcStats);
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
    public long getCreationTime() {
        return 0;
    }

    @Override
    public void fromJson(JsonObject json) {
        throw new UnsupportedOperationException();
    }

    static void checkFreeMemory(long size) {
        long totalMem = totalPhysicalMemory();
        if (totalMem < 0) {
            return;
        }
        long freeMem = freePhysicalMemory();
        if (freeMem < 0) {
            return;
        }
        if (size > freeMem) {
            throw new NativeMemoryOutOfMemoryError("Not enough free physical memory available!"
                    + " Cannot allocate " + MemorySize.toPrettyString(size) + "!"
                    + " Total physical memory: " + MemorySize.toPrettyString(totalMem)
                    + ", Free physical memory: " + MemorySize.toPrettyString(freeMem)
            );
        }
    }
}
