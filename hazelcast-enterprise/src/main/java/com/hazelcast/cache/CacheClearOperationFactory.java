package com.hazelcast.cache;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * TODO pass the arguments to operation!!!
 */
public class CacheClearOperationFactory implements OperationFactory, IdentifiedDataSerializable {

    private String name;

    private Set<Data> keySet;

    boolean isRemoveAll;

    Integer completionId;

    public CacheClearOperationFactory() {
    }

    public CacheClearOperationFactory(String name, Set<Data> keySet, boolean isRemoveAll, Integer completionId) {
        this.name = name;
        this.keySet = keySet;
        this.isRemoveAll = isRemoveAll;
        this.completionId = completionId;
    }

    @Override
    public Operation createOperation() {
        return new CacheClearOperation(name);
    }

    @Override
    public int getFactoryId() {
        return EnterpriseCacheDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EnterpriseCacheDataSerializerHook.CLEAR_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeBoolean(isRemoveAll);
        out.writeInt(completionId);
        if (keySet == null) {
            out.writeInt(0);
            return;
        }
        out.writeInt(keySet.size());
        for (Data data : keySet) {
            out.writeData(data);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        isRemoveAll = in.readBoolean();
        completionId = in.readInt();
        int size = in.readInt();
        keySet = new HashSet<Data>(size);
        for (int i = 0; i < size; i++) {
            keySet.add(in.readData());
        }
    }
}
