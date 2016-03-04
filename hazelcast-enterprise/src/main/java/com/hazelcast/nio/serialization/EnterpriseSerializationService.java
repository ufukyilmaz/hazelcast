package com.hazelcast.nio.serialization;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.memory.JvmMemoryManager;

public interface EnterpriseSerializationService extends SerializationService, OffHeapSerializationService {

    <B extends Data> B toData(Object obj, DataType type);

    <B extends Data> B toNativeData(Object obj, JvmMemoryManager memoryManager);

    <B extends Data> B toData(Object obj, DataType type, PartitioningStrategy strategy);

    <B extends Data> B convertData(Data data, DataType type);

    <B extends Data> B convertToNativeData(Data data, JvmMemoryManager memoryManager);

    void disposeData(Data data, JvmMemoryManager memoryManager);

    <T> T toObject(Object data, JvmMemoryManager memoryManager);

    JvmMemoryManager getMemoryManager();

    JvmMemoryManager getMemoryManagerToUse();
}
