package com.hazelcast.memory;

public interface MemoryStats {

    long getTotalPhysical();

    long getFreePhysical();

    long getMaxHeap();
    
    long getCommittedHeap();

    long getUsedHeap();

    long getFreeHeap();

    long getMaxOffHeap();

    long getCommittedOffHeap();

    long getUsedOffHeap();

    long getFreeOffHeap();

    GCStats getGCStats();

    SerializableMemoryStats asSerializable();

}
