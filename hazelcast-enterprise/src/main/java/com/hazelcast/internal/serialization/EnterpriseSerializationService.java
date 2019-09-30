package com.hazelcast.internal.serialization;

import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.nio.serialization.Data;

public interface EnterpriseSerializationService extends InternalSerializationService {

    <B extends Data> B toNativeData(Object obj, MemoryAllocator malloc);

    <B extends Data> B convertToNativeData(Data data, MemoryAllocator malloc);

    void disposeData(Data data, MemoryAllocator memoryAllocator);

    <T> T toObject(Object data, MemoryAllocator memoryAllocator);

    MemoryAllocator getCurrentMemoryAllocator();

    HazelcastMemoryManager getMemoryManager();
}
