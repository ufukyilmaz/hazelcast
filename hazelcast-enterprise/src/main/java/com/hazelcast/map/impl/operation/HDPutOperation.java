package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.MutatingOperation;

public class HDPutOperation extends HDBasePutOperation implements IdentifiedDataSerializable, MutatingOperation {

    public HDPutOperation() {
    }

    public HDPutOperation(String name, Data dataKey, Data value, long ttl, long maxIdle) {
        super(name, dataKey, value, ttl, maxIdle);
    }

    @Override
    protected void runInternal() {
        oldValue = mapServiceContext.toData(recordStore.put(dataKey, dataValue, ttl, maxIdle));
    }

    @Override
    public Object getResponse() {
        return oldValue;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.PUT;
    }
}
