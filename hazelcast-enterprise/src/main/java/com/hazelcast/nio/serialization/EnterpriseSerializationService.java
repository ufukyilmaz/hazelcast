package com.hazelcast.nio.serialization;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.memory.HazelcastMemoryManager;

public interface EnterpriseSerializationService extends SerializationService, OffHeapSerializationService {

    <B extends Data> B toData(Object obj, DataType type);

    <B extends Data> B toNativeData(Object obj, HazelcastMemoryManager memoryManager);

    <B extends Data> B toData(Object obj, DataType type, PartitioningStrategy strategy);

    <B extends Data> B convertData(Data data, DataType type);

    <B extends Data> B convertToNativeData(Data data, HazelcastMemoryManager memoryManager);

    void disposeData(Data data, HazelcastMemoryManager memoryManager);

    <T> T toObject(Object data, HazelcastMemoryManager memoryManager);

    HazelcastMemoryManager getMemoryManager();

    HazelcastMemoryManager getMemoryManagerToUse();
}
