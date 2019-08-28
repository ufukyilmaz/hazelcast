package com.hazelcast.internal.memory.impl;

import com.hazelcast.nio.Disposable;

public interface LibMalloc extends Disposable {

    /**
     * NULL pointer address.
     */
    long NULL_ADDRESS = 0L;

    long malloc(long size);

    long realloc(long address, long size);

    void free(long address);
}
