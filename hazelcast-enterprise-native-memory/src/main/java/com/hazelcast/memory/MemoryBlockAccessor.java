package com.hazelcast.memory;

/**
 * TODO: need refactoring & cleanup!
 *
 * @author mdogan 11/02/14
 */
public interface MemoryBlockAccessor<V extends MemoryBlock> {

    boolean isEqual(long address, V value);

    boolean isEqual(long address1, long address2);

    V read(long address);

    long dispose(V block);

    long dispose(long address);

}
