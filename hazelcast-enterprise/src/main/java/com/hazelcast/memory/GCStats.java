package com.hazelcast.memory;

public interface GCStats {

    long getMajorCollectionCount();

    long getMajorCollectionTime();

    long getMinorCollectionCount();

    long getMinorCollectionTime();

    long getUnknownCollectionCount();

    long getUnknownCollectionTime();

    SerializableGCStats asSerializable();
}
