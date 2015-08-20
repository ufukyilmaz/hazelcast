package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

/**
 * @author mdogan 06/02/14
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
        out.writeUTF(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }

    @Override
    public int getFactoryId() {
        return HiDensityCacheDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.SIZE_FACTORY;
    }

}
