package com.hazelcast.internal.memory;

/**
 * TODO: need refactoring & cleanup!
 */
public interface MemoryBlockAccessor<V extends MemoryBlock> {

    boolean isEqual(long address, V value);

    boolean isEqual(long address1, long address2);

    V read(long address);

    long dispose(V block);

    long dispose(long address);
}
