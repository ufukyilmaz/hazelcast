package com.hazelcast.nio.serialization;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.memory.MemoryManager;

public interface EnterpriseSerializationService extends SerializationService {

    <B extends Data> B toData(Object obj, DataType type);
    <B extends Data> B toNativeData(Object obj, MemoryManager memoryManager);

    <B extends Data> B toData(Object obj, DataType type, PartitioningStrategy strategy);

    <B extends Data> B convertData(Data data, DataType type);
    <B extends Data> B convertToNativeData(Data data, MemoryManager memoryManager);

    void disposeData(Data data, MemoryManager memoryManager);

    <T> T toObject(Object data, MemoryManager memoryManager);

    MemoryManager getMemoryManager();
    MemoryManager getMemoryManagerToUse();

}
