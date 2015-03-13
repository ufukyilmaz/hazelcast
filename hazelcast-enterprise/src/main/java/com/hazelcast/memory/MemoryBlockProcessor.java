package com.hazelcast.memory;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;

/**
 * @author sozal 17/11/14
 */
public interface MemoryBlockProcessor<V extends MemoryBlock>
        extends MemoryBlockAccessor<V> {

    Data toData(Object obj, DataType dataType);
    Data convertData(Data data, DataType dataType);
    void disposeData(Data data);

    long allocate(long size);
    void free(long address, long size);

    MemoryAllocator unwrapMemoryAllocator();

}
