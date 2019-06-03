package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;

/**
 * Gets the value to which the specified key is mapped, or {@code null} if this cache contains no mapping for the key.
 */
public class CacheGetOperation
        extends KeyBasedHiDensityCacheOperation
        implements ReadonlyOperation {

    private ExpiryPolicy expiryPolicy;

    public CacheGetOperation() {
    }

    public CacheGetOperation(String name, Data key, ExpiryPolicy expiryPolicy) {
        super(name, key);
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    protected void runInternal() {
        response = recordStore != null ? recordStore.get(key, expiryPolicy) : null;
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();
        dispose();
    }

    @Override
    protected void disposeInternal(EnterpriseSerializationService serializationService) {
        serializationService.disposeData(key);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(expiryPolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        expiryPolicy = in.readObject();
    }

    @Override
    public int getClassId() {
        return HiDensityCacheDataSerializerHook.GET;
    }
}
