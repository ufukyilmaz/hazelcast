package com.hazelcast.internal.memory;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;

/**
 * @param <V> type of MemoryBlock
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
