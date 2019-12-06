package com.hazelcast.test.compatibility;

import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;

public class SamplingEnterpriseSerializationService extends SamplingSerializationService
        implements EnterpriseSerializationService {

    private final EnterpriseSerializationService enterpriseSerializationService;

    SamplingEnterpriseSerializationService(EnterpriseSerializationService delegate) {
        super(delegate);
        this.enterpriseSerializationService = delegate;
    }

    @Override
    public <B extends Data> B toData(Object obj, DataType type) {
        B data = enterpriseSerializationService.toData(obj, type);
        sampleObject(obj, data == null ? null : data.toByteArray());
        return data;
    }

    @Override
    public <B extends Data> B toNativeData(Object obj, MemoryAllocator malloc) {
        B data = enterpriseSerializationService.toNativeData(obj, malloc);
        sampleObject(obj, data == null ? null : data.toByteArray());
        return data;
    }

    @Override
    public <B extends Data> B toData(Object obj, DataType type, PartitioningStrategy strategy) {
        B data = enterpriseSerializationService.toData(obj, type, strategy);
        sampleObject(obj, data == null ? null : data.toByteArray());
        return data;
    }

    @Override
    public <B extends Data> B convertData(Data data, DataType type) {
        return enterpriseSerializationService.convertData(data, type);
    }

    @Override
    public <B extends Data> B convertToNativeData(Data data, MemoryAllocator malloc) {
        return enterpriseSerializationService.convertToNativeData(data, malloc);
    }

    @Override
    public void disposeData(Data data, MemoryAllocator memoryAllocator) {
        enterpriseSerializationService.disposeData(data, memoryAllocator);
    }

    @Override
    public <T> T toObject(Object data, MemoryAllocator memoryAllocator) {
        return enterpriseSerializationService.toObject(data, memoryAllocator);
    }

    @Override
    public MemoryAllocator getCurrentMemoryAllocator() {
        return enterpriseSerializationService.getCurrentMemoryAllocator();
    }

    @Override
    public HazelcastMemoryManager getMemoryManager() {
        return enterpriseSerializationService.getMemoryManager();
    }
}
