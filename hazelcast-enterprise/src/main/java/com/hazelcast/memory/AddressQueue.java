package com.hazelcast.memory;

/**
 * @author mdogan 07/01/14
 */
interface AddressQueue {

    long INVALID_ADDRESS = MemoryManager.NULL_ADDRESS;

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
