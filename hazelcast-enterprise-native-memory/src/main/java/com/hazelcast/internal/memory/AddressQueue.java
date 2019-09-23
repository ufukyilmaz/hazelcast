package com.hazelcast.internal.memory;

interface AddressQueue {

    long INVALID_ADDRESS = HazelcastMemoryManager.NULL_ADDRESS;

    int getIndex();

    long acquire();

    boolean release(long memory);

    int getMemorySize();

    int capacity();

    int remaining();

    boolean beforeCompaction();

    void afterCompaction();

    void destroy();
}
