package com.hazelcast.memory;

import com.hazelcast.com.eclipsesource.json.JsonObject;
import com.hazelcast.monitor.LocalGCStats;
import com.hazelcast.monitor.LocalMemoryStats;
import com.hazelcast.monitor.impl.LocalGCStatsImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.memory.MemoryStatsSupport.freePhysicalMemory;
import static com.hazelcast.memory.MemoryStatsSupport.getHeapMemoryUsage;
import static com.hazelcast.memory.MemoryStatsSupport.totalPhysicalMemory;

class StandardMemoryStats implements LocalMemoryStats {

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
    public long getMaxNativeMemory() {
        return 0;
    }

    @Override
    public long getCommittedNativeMemory() {
        return 0;
    }

    @Override
    public long getUsedNativeMemory() {
        return 0;
    }

    @Override
    public long getFreeNativeMemory() {
        return 0L;
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
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(getTotalPhysical());
        out.writeLong(getFreePhysical());
        out.writeLong(getMaxNativeMemory());
        out.writeLong(getCommittedNativeMemory());
        out.writeLong(getUsedNativeMemory());
        out.writeLong(getMaxHeap());
        out.writeLong(getCommittedHeap());
        out.writeLong(getUsedHeap());
        getGCStats().writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {

    }


    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("creationTime", getCreationTime());
        root.add("totalPhysical", getTotalPhysical());
        root.add("freePhysical", getFreePhysical());
        root.add("maxNativeMemory", getMaxNativeMemory());
        root.add("committedNativeMemory", getCommittedNativeMemory());
        root.add("usedNativeMemory", getUsedNativeMemory());
        root.add("freeNativeMemory", getFreeNativeMemory());
        root.add("maxHeap", getMaxHeap());
        root.add("committedHeap", getCommittedHeap());
        root.add("usedHeap", getUsedHeap());
        root.add("gcStats", getGCStats().toJson());
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {

    }
}
