package com.hazelcast.memory;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class SerializableGCStats extends MutableGCStats implements GCStats, DataSerializable {

    private long minorCount;
    private long minorTime;
    private long majorCount;
    private long majorTime;
    private long unknownCount;
    private long unknownTime;

    public SerializableGCStats() {
    }

    @Override
    public long getMajorCollectionCount() {
        return majorCount;
    }

    @Override
    public long getMajorCollectionTime() {
        return majorTime;
    }

    @Override
    public long getMinorCollectionCount() {
        return minorCount;
    }

    @Override
    public long getMinorCollectionTime() {
        return minorTime;
    }

    @Override
    public long getUnknownCollectionCount() {
        return unknownCount;
    }

    @Override
    public long getUnknownCollectionTime() {
        return unknownTime;
    }

    void setMinorCount(long minorCount) {
        this.minorCount = minorCount;
    }

    void setMinorTime(long minorTime) {
        this.minorTime = minorTime;
    }

    void setMajorCount(long majorCount) {
        this.majorCount = majorCount;
    }

    void setMajorTime(long majorTime) {
        this.majorTime = majorTime;
    }

    void setUnknownCount(long unknownCount) {
        this.unknownCount = unknownCount;
    }

    void setUnknownTime(long unknownTime) {
        this.unknownTime = unknownTime;
    }

    @Override
    public SerializableGCStats asSerializable() {
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeLong(majorCount);
        out.writeLong(majorTime);
        out.writeLong(minorCount);
        out.writeLong(minorTime);
        out.writeLong(unknownCount);
        out.writeLong(unknownTime);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        majorCount = in.readLong();
        majorTime = in.readLong();
        minorCount = in.readLong();
        minorTime = in.readLong();
        unknownCount = in.readLong();
        unknownTime = in.readLong();
    }
}
