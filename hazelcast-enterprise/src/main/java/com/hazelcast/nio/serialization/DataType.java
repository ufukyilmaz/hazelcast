package com.hazelcast.nio.serialization;

/**
 * Defines type of the Data, on-heap or off-heap
 */
public enum DataType {

    /**
     * On-heap data type
     */
    HEAP,

    /**
     * Off-heap data type
     */
    OFFHEAP
}
