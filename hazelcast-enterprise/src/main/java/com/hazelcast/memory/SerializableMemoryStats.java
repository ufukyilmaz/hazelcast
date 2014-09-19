package com.hazelcast.memory;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class SerializableMemoryStats implements MemoryStats, DataSerializable {

    private long totalPhysical;

    private long freePhysical;

    private long maxOffHeap;

    private long committedOffHeap;

    private long usedOffHeap;

    private long maxHeap;

    private long committedHeap;

    private long usedHeap;

    private SerializableGCStats gcStats;

    public SerializableMemoryStats() {
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(totalPhysical);
        out.writeLong(freePhysical);
        out.writeLong(maxOffHeap);
        out.writeLong(committedOffHeap);
        out.writeLong(usedOffHeap);
        out.writeLong(maxHeap);
        out.writeLong(committedHeap);
        out.writeLong(usedHeap);
        if (gcStats == null) {
            gcStats = new SerializableGCStats();
        }
        gcStats.writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        totalPhysical = in.readLong();
        freePhysical = in.readLong();
        maxOffHeap = in.readLong();
        committedOffHeap = in.readLong();
        usedOffHeap = in.readLong();
        maxHeap = in.readLong();
        committedHeap = in.readLong();
        usedHeap = in.readLong();
        gcStats = new SerializableGCStats();
        gcStats.readData(in);
    }

    @Override
    public long getTotalPhysical() {
        return totalPhysical;
    }

    void setTotalPhysical(long totalPhysical) {
        this.totalPhysical = totalPhysical;
    }

    @Override
    public long getFreePhysical() {
        return freePhysical;
    }

    void setFreePhysical(long freePhysical) {
        this.freePhysical = freePhysical;
    }

    @Override
    public long getMaxOffHeap() {
        return maxOffHeap;
    }

    void setMaxOffHeap(long maxOffHeap) {
        this.maxOffHeap = maxOffHeap;
    }

    @Override
    public long getCommittedOffHeap() {
        return committedOffHeap;
    }

    void setCommittedOffHeap(long allocated) {
        this.committedOffHeap = allocated;
    }

    @Override
    public long getUsedOffHeap() {
        return usedOffHeap;
    }

    void setUsedOffHeap(long used) {
        this.usedOffHeap = used;
    }

    @Override
    public long getFreeOffHeap() {
        return maxOffHeap - usedOffHeap;
    }

    @Override
    public long getMaxHeap() {
        return maxHeap;
    }

    @Override
    public long getCommittedHeap() {
        return committedHeap;
    }

    @Override
    public long getUsedHeap() {
        return usedHeap;
    }

    void setMaxHeap(long maxHeap) {
        this.maxHeap = maxHeap;
    }

    void setCommittedHeap(long committedHeap) {
        this.committedHeap = committedHeap;
    }

    void setUsedHeap(long usedHeap) {
        this.usedHeap = usedHeap;
    }

    @Override
    public long getFreeHeap() {
        return maxHeap - usedHeap;
    }

    @Override
    public SerializableGCStats getGCStats() {
        return gcStats;
    }

    void setGcStats(SerializableGCStats gcStats) {
        this.gcStats = gcStats;
    }

    @Override
    public SerializableMemoryStats asSerializable() {
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SerializableMemoryStats{");
        sb.append("totalPhysical=").append(totalPhysical);
        sb.append(", freePhysical=").append(freePhysical);
        sb.append(", maxOffHeap=").append(maxOffHeap);
        sb.append(", committedOffHeap=").append(committedOffHeap);
        sb.append(", usedOffHeap=").append(usedOffHeap);
        sb.append(", maxHeap=").append(maxHeap);
        sb.append(", committedHeap=").append(committedHeap);
        sb.append(", usedHeap=").append(usedHeap);
        sb.append(", gcStats=").append(gcStats);
        sb.append('}');
        return sb.toString();
    }
}
