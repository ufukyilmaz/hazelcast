package com.hazelcast.cache.impl.hidensity.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;

import java.io.IOException;

/**
 * Creates {@link CacheSizeOperation}s.
 */
public class CacheSizeOperationFactory
        implements OperationFactory, IdentifiedDataSerializable {

    private String name;

    public CacheSizeOperationFactory() {
    }

    public CacheSizeOperationFactory(String name) {
        this.name = name;
    }

    @Override
    public Operation createOperation() {
        return new CacheSizeOperation(name);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
    }

    @Override
    public int getFactoryId() {
        return HiDensityCacheDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return HiDensityCacheDataSerializerHook.SIZE_FACTORY;
    }
}
