package com.hazelcast.nio.serialization;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.memory.HazelcastMemoryManager;

public interface EnterpriseSerializationService extends InternalSerializationService {

    <B extends Data> B toNativeData(Object obj, MemoryAllocator malloc);

    <B extends Data> B convertToNativeData(Data data, MemoryAllocator malloc);

    void disposeData(Data data, MemoryAllocator memoryAllocator);

    <T> T toObject(Object data, MemoryAllocator memoryAllocator);

    MemoryAllocator getCurrentMemoryAllocator();

    HazelcastMemoryManager getMemoryManager();
}
