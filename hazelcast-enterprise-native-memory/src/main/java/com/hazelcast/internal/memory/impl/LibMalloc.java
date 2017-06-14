package com.hazelcast.internal.memory.impl;

public interface LibMalloc {

    /**
     * NULL pointer address.
     */
    long NULL_ADDRESS = 0L;

    long malloc(long size);

    long realloc(long address, long size);

    void free(long address);
}
