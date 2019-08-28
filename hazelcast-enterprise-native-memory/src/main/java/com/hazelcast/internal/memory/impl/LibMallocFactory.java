package com.hazelcast.internal.memory.impl;

/**
 * A factory to create {@link LibMalloc} object for memory manager.
 */
public interface LibMallocFactory {

    /**
     * Creates a {@link LibMalloc} backed by native memory of specified {@code size}.
     * @param size the size of the native memory
     * @return the {@link LibMalloc} object
     */
    LibMalloc create(long size);
}
