package com.hazelcast.nio.serialization;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.EnterpriseObjectDataInput;

public interface EnterpriseSerializationService extends SerializationService {

    <B extends Data> B toData(Object obj, DataType type);

    <B extends Data> B toData(Object obj, DataType type, PartitioningStrategy strategy);

    <B extends Data> B readData(EnterpriseObjectDataInput in, DataType type);

    <B extends Data> B tryReadData(EnterpriseObjectDataInput in, DataType type);

    <B extends Data> B convertData(Data data, DataType type);

    MemoryManager getMemoryManager();
}
