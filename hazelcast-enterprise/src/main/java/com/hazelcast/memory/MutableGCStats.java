package com.hazelcast.memory;

abstract class MutableGCStats implements GCStats {

    abstract void setMinorCount(long minorCount);

    abstract void setMinorTime(long minorTime);

    abstract void setMajorCount(long majorCount);

    abstract void setMajorTime(long majorTime);

    abstract void setUnknownCount(long unknownCount);

    abstract void setUnknownTime(long unknownTime);
}
