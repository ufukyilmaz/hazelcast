package com.hazelcast.nio.serialization;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.EnterpriseObjectDataInput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

public interface EnterpriseSerializationService extends SerializationService {

    <B extends Data> B toData(Object obj, DataType type);
    <B extends Data> B toNativeData(Object obj, MemoryManager memoryManager);

    <B extends Data> B toData(Object obj, DataType type, PartitioningStrategy strategy);
    <B extends Data> B toNativeData(Object obj, PartitioningStrategy strategy, MemoryManager memoryManager);

    <B extends Data> B readData(EnterpriseObjectDataInput in, DataType type);
    <B extends Data> B readNativeData(EnterpriseObjectDataInput in, MemoryManager memoryManager);

    <B extends Data> B tryReadData(EnterpriseObjectDataInput in, DataType type);
    <B extends Data> B tryReadNativeData(EnterpriseObjectDataInput in, MemoryManager memoryManager);

    <B extends Data> B convertData(Data data, DataType type);
    <B extends Data> B convertToNativeData(Data data, MemoryManager memoryManager);

    void disposeData(Data data);
    void disposeData(Data data, MemoryManager memoryManager);

    <T> T toObject(Object data, MemoryManager memoryManager);
    void writeObject(ObjectDataOutput out, Object obj, MemoryManager memoryManager);
    <T> T readObject(ObjectDataInput in, MemoryManager memoryManager);

    void writeData(ObjectDataOutput out, Data data, MemoryManager memoryManager);
    <B extends Data> B readData(ObjectDataInput in, MemoryManager memoryManager);

    MemoryManager getMemoryManager();

}
