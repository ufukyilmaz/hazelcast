package com.hazelcast.test.compatibility;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

/**
 * Enterprise serialization service that intercepts and samples serialized objects.
 */
public class SamplingEnterpriseSerializationService extends SamplingSerializationService
        implements EnterpriseSerializationService {

    public SamplingEnterpriseSerializationService(InternalSerializationService delegate) {
        super(delegate);
    }

    EnterpriseSerializationService getEnterpriseSerializationService() {
        return (EnterpriseSerializationService) delegate;
    }

    @Override
    public <B extends Data> B toData(Object obj, DataType type) {
        B data = getEnterpriseSerializationService().toData(obj, type);
        sampleObject(obj, data.toByteArray());
        return data;
    }

    @Override
    public <B extends Data> B toNativeData(Object obj, MemoryAllocator malloc) {
        B data = getEnterpriseSerializationService().toNativeData(obj, malloc);
        sampleObject(obj, data.toByteArray());
        return data;
    }

    @Override
    public <B extends Data> B toData(Object obj, DataType type, PartitioningStrategy strategy) {
        B data = getEnterpriseSerializationService().toData(obj, type, strategy);
        sampleObject(obj, data.toByteArray());
        return data;
    }

    @Override
    public <B extends Data> B convertData(Data data, DataType type) {
        return getEnterpriseSerializationService().convertData(data, type);
    }

    @Override
    public <B extends Data> B convertToNativeData(Data data, MemoryAllocator malloc) {
        return getEnterpriseSerializationService().convertToNativeData(data, malloc);
    }

    @Override
    public void disposeData(Data data, MemoryAllocator memoryAllocator) {
        getEnterpriseSerializationService().disposeData(data, memoryAllocator);
    }

    @Override
    public <T> T toObject(Object data, MemoryAllocator memoryAllocator) {
        return getEnterpriseSerializationService().toObject(data, memoryAllocator);
    }

    @Override
    public MemoryAllocator getCurrentMemoryAllocator() {
        return getEnterpriseSerializationService().getCurrentMemoryAllocator();
    }

    @Override
    public HazelcastMemoryManager getMemoryManager() {
        return getEnterpriseSerializationService().getMemoryManager();
    }
}
