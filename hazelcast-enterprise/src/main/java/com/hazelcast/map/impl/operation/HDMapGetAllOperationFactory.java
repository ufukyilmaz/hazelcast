package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HDMapGetAllOperationFactory implements OperationFactory {

    private String name;
    private List<Data> keys = new ArrayList<>();

    public HDMapGetAllOperationFactory() {
    }

    public HDMapGetAllOperationFactory(String name, List<Data> keys) {
        this.name = name;
        this.keys = keys;
    }

    @Override
    public Operation createOperation() {
        return new HDGetAllOperation(name, keys);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(keys.size());
        for (Data key : keys) {
            out.writeData(key);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            Data data = in.readData();
            keys.add(data);
        }
    }

    @Override
    public int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.GET_ALL_FACTORY;
    }
}
