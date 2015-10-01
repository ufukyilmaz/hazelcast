package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.ReadonlyOperation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;

/**
 * @author mdogan 05/02/14
 */
public class CacheGetOperation
        extends AbstractKeyBasedHiDensityCacheOperation
        implements ReadonlyOperation {

    private ExpiryPolicy expiryPolicy;

    public CacheGetOperation() {
    }

    public CacheGetOperation(String name, Data key, ExpiryPolicy expiryPolicy) {
        super(name, key);
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    protected void runInternal() throws Exception {
        response = cache != null ? cache.get(key, expiryPolicy) : null;
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
    public int getId() {
        return HiDensityCacheDataSerializerHook.GET;
    }

}
