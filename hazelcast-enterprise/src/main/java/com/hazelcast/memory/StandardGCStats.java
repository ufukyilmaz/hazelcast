package com.hazelcast.memory;

/**
 * @author mdogan 12/04/14
 */
public class StandardGCStats extends MutableGCStats implements GCStats {

    private volatile long minorCount;
    private volatile long minorTime;
    private volatile long majorCount;
    private volatile long majorTime;
    private volatile long unknownCount;
    private volatile long unknownTime;

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

    public void refresh() {
        GCStatsSupport.fill(this);
    }

    @Override
    public SerializableGCStats asSerializable() {
        refresh();
        SerializableGCStats stats = new SerializableGCStats();
        stats.setMajorCount(majorCount);
        stats.setMajorTime(majorTime);
        stats.setMinorCount(minorCount);
        stats.setMinorTime(minorTime);
        stats.setUnknownCount(unknownCount);
        stats.setUnknownTime(unknownTime);
        return stats;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(":: GC STATS :: ").append('\n');

        sb.append("MinorGC -> Count: ").append(minorCount).append(", Time (ms): ").append(minorTime)
            .append(", MajorGC -> Count: ").append(majorCount).append(", Time (ms): ").append(majorTime);

        if (unknownCount > 0) {
            sb.append(", UnknownGC -> Count: ").append(unknownCount)
              .append(", Time (ms): ").append(unknownTime);
        }

        sb.append('\n');
        return sb.toString();
    }
}
