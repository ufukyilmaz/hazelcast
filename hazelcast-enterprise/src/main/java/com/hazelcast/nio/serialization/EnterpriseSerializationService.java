package com.hazelcast.nio.serialization;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.spi.memory.MemoryAllocator;

public interface EnterpriseSerializationService extends SerializationService {

    <B extends Data> B toData(Object obj, DataType type);

    <B extends Data> B toNativeData(Object obj, MemoryAllocator malloc);

    <B extends Data> B toData(Object obj, DataType type, PartitioningStrategy strategy);

    <B extends Data> B convertData(Data data, DataType type);

    <B extends Data> B convertToNativeData(Data data, MemoryAllocator malloc);

    void disposeData(Data data, MemoryAllocator memoryAllocator);

    <T> T toObject(Object data, MemoryAllocator memoryAllocator);

    MemoryAllocator getCurrentMemoryAllocator();

    HazelcastMemoryManager getMemoryManager();
}
