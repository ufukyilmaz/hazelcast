package com.hazelcast.memory;

import com.hazelcast.com.eclipsesource.json.JsonObject;
import com.hazelcast.memory.error.OffHeapOutOfMemoryError;
import com.hazelcast.monitor.LocalMemoryStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.memory.EnterpriseMemoryStatsSupport.checkFreeMemory;

/**
 * @author mdogan 10/02/14
 */
class OffHeapMemoryStats extends StandardMemoryStats implements LocalMemoryStats {

    private static final boolean ASSERTS_ENABLED;

    static {
        ASSERTS_ENABLED = OffHeapMemoryStats.class.desiredAssertionStatus();
    }

    private final long maxOffHeap;

    private final AtomicLong committedOffHeap = new AtomicLong();

    private final AtomicLong internalFragmentation = new AtomicLong();

    OffHeapMemoryStats(long maxOffHeap) {
        this.maxOffHeap = maxOffHeap;
    }

    @Override
    public final long getMaxNativeMemory() {
        return maxOffHeap;
    }

    @Override
    public final long getCommittedNativeMemory() {
        return committedOffHeap.get();
    }

    @Override
    public long getUsedNativeMemory() {
        return getCommittedNativeMemory();
    }

    @Override
    public final long getFreeNativeMemory() {
        long free = maxOffHeap - getUsedNativeMemory();
        return free > 0 ? free : 0L;
    }

    final void checkOffHeapAllocation(long size) {
        checkCommittedOffHeap(size);
        checkFreeMemory(size);
    }

    private void checkCommittedOffHeap(long size) {
        if (size > 0) {
            long currentAllocated = committedOffHeap.get();
            if (maxOffHeap < (currentAllocated + size)) {
                throw new OffHeapOutOfMemoryError("Not enough contiguous memory available! " +
                        " Cannot allocate " + MemorySize.toPrettyString(size) + "!" +
                        " Max OffHeap: " + MemorySize.toPrettyString(maxOffHeap) +
                        ", Committed OffHeap: " + MemorySize.toPrettyString(currentAllocated) +
                        ", Used OffHeap: " + MemorySize.toPrettyString(getUsedNativeMemory())
                );
            }
        }
    }

    final void addCommittedOffHeap(long size) {
        committedOffHeap.addAndGet(size);
    }

    final void addInternalFragmentation(long size) {
        if (ASSERTS_ENABLED) {
            internalFragmentation.addAndGet(size);
        }
    }

    @Override
    public final String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(":: MEMORY STATS :: ").append('\n');

        sb.append("Total Physical: ").append(MemorySize.toPrettyString(getTotalPhysical()));
        sb.append(", Free Physical: ").append(MemorySize.toPrettyString(getFreePhysical()));
        sb.append(", Max Heap: ").append(MemorySize.toPrettyString(getMaxHeap()));
        sb.append(", Committed Heap: ").append(MemorySize.toPrettyString(getCommittedHeap()));
        sb.append(", Used Heap: ").append(MemorySize.toPrettyString(getUsedHeap()));
        sb.append(", Free Heap: ").append(MemorySize.toPrettyString(getFreeHeap()));
        sb.append(", Max OffHeap: ").append(MemorySize.toPrettyString(getMaxNativeMemory()));
        sb.append(", Committed OffHeap: ").append(MemorySize.toPrettyString(getCommittedNativeMemory()));
        sb.append(", Used OffHeap: ").append(MemorySize.toPrettyString(getUsedNativeMemory()));
        sb.append(", Free OffHeap: ").append(MemorySize.toPrettyString(getFreeNativeMemory()));
        appendAdditionalToString(sb);
        if (ASSERTS_ENABLED) {
            sb.append(", Internal Fragmentation: ").append(MemorySize.toPrettyString(internalFragmentation.get()));
        }
        sb.append('\n');
        sb.append(getGCStats());
        return sb.toString();
    }

    void appendAdditionalToString(StringBuilder sb) {
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
}
