package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.Operation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;

/**
 * @author mdogan 05/02/14
 */
public class CachePutOperation extends BackupAwareHiDensityCacheOperation {

    private Data value;
    private boolean get;
    private ExpiryPolicy expiryPolicy;

    public CachePutOperation() {
    }

    public CachePutOperation(String name, Data key, Data value,
                             ExpiryPolicy expiryPolicy) {
        super(name, key);
        this.value = value;
        this.expiryPolicy = expiryPolicy;
        this.get = false;
    }

    public CachePutOperation(String name, Data key, Data value,
                             ExpiryPolicy expiryPolicy, boolean get) {
        super(name, key);
        this.value = value;
        this.expiryPolicy = expiryPolicy;
        this.get = get;
    }

    @Override
    public void runInternal() throws Exception {
        if (get) {
            response = cache.getAndPut(key, value, expiryPolicy, getCallerUuid(), completionId);
        } else {
            cache.put(key, value, expiryPolicy, getCallerUuid(), completionId);
            response = null;
        }
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutBackupOperation(name, key, value, expiryPolicy);
    }

    @Override
    protected void disposeInternal(SerializationService serializationService) {
        serializationService.disposeData(value);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(get);
        out.writeObject(expiryPolicy);
        out.writeData(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        get = in.readBoolean();
        expiryPolicy = in.readObject();
        value = readOperationData(in);
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.PUT;
    }
}
