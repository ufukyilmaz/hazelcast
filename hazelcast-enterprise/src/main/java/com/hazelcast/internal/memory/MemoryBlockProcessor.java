package com.hazelcast.internal.memory;

import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.memory.MemoryBlockAccessor;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;

/**
 * @author sozal 17/11/14
 *
 * @param <V> the type of MemoryBlock.
 */
public interface MemoryBlockProcessor<V extends MemoryBlock>
        extends MemoryBlockAccessor<V> {

    Data toData(Object obj, DataType dataType);
    Object toObject(Object data);
    Data convertData(Data data, DataType dataType);
    void disposeData(Data data);

    long allocate(long size);
    void free(long address, long size);

    MemoryAllocator unwrapMemoryAllocator();

}
