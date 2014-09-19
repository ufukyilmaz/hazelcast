package com.hazelcast.cache;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

/**
 * @author mdogan 06/02/14
 */
public class CacheClearOperationFactory implements OperationFactory, IdentifiedDataSerializable {

    private String name;

    public CacheClearOperationFactory() {
    }

    public CacheClearOperationFactory(String name) {
        this.name = name;
    }

    @Override
    public Operation createOperation() {
        return new CacheClearOperation(name);
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.CLEAR_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }
}
