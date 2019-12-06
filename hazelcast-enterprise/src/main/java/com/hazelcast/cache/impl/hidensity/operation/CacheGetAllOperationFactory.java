package com.hazelcast.cache.impl.hidensity.operation;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Factory implementation for {@link com.hazelcast.cache.impl.operation.CacheGetAllOperation}.
 *
 * @see com.hazelcast.spi.OperationFactory
 */
public class CacheGetAllOperationFactory
        implements OperationFactory, IdentifiedDataSerializable {

    private String name;
    private Set<Data> keys = new HashSet<Data>();
    private ExpiryPolicy expiryPolicy;

    public CacheGetAllOperationFactory() {
    }

    public CacheGetAllOperationFactory(String name, Set<Data> keys,
                                       ExpiryPolicy expiryPolicy) {
        this.name = name;
        this.keys = keys;
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    public Operation createOperation() {
        return new CacheGetAllOperation(name, keys, expiryPolicy);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeObject(expiryPolicy);
        out.writeInt(keys.size());
        for (Data key : keys) {
            IOUtil.writeData(out, key);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        expiryPolicy = in.readObject();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            Data data = IOUtil.readData(in);
            keys.add(data);
        }
    }

    @Override
    public int getFactoryId() {
        return HiDensityCacheDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return HiDensityCacheDataSerializerHook.GET_ALL_FACTORY;
    }

}
