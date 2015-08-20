package com.hazelcast.cache.hidensity.operation;

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
 * Factory implementation for {@link com.hazelcast.cache.impl.operation.CacheLoadAllOperation}.
 *
 * @see com.hazelcast.spi.OperationFactory
 */
public class CacheLoadAllOperationFactory
        implements OperationFactory, IdentifiedDataSerializable {

    private String name;
    private Set<Data> keys;
    private boolean replaceExistingValues;

    public CacheLoadAllOperationFactory(String name, Set<Data> keys,
                                        boolean replaceExistingValues) {
        this.name = name;
        this.keys = keys;
        this.replaceExistingValues = replaceExistingValues;
    }

    public CacheLoadAllOperationFactory() {
    }

    @Override
    public Operation createOperation() {
        return new CacheLoadAllOperation(name, keys, replaceExistingValues);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeBoolean(replaceExistingValues);
        out.writeBoolean(keys != null);
        if (keys != null) {
            out.writeInt(keys.size());
            for (Data key : keys) {
                out.writeData(key);
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        replaceExistingValues = in.readBoolean();
        boolean isKeysNotNull = in.readBoolean();
        if (isKeysNotNull) {
            int size = in.readInt();
            keys = new HashSet<Data>(size);
            for (int i = 0; i < size; i++) {
                Data key = in.readData();
                keys.add(key);
            }
        }
    }

    @Override
    public int getFactoryId() {
        return HiDensityCacheDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.LOAD_ALL_FACTORY;
    }

}
