package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.MutatingOperation;

public class HDPutOperation extends HDBasePutOperation implements IdentifiedDataSerializable, MutatingOperation {

    public HDPutOperation() {
    }

    public HDPutOperation(String name, Data dataKey, Data value, long ttl) {
        super(name, dataKey, value, ttl);
    }

    @Override
    protected void runInternal() {
        dataOldValue = mapServiceContext.toData(recordStore.put(dataKey, dataValue, ttl));
    }

    @Override
    public Object getResponse() {
        return dataOldValue;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.PUT;
    }
}
