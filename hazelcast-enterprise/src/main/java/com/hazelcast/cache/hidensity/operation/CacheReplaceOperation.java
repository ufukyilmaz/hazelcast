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
public class CacheReplaceOperation extends BackupAwareHiDensityCacheOperation {

    private Data value;
    private Data currentValue;
    private ExpiryPolicy expiryPolicy;

    public CacheReplaceOperation() {
    }

    public CacheReplaceOperation(String name, Data key, Data oldValue,
                                 Data newValue, ExpiryPolicy expiryPolicy) {
        super(name, key);
        this.value = newValue;
        this.currentValue = oldValue;
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    public void runInternal() throws Exception {
        if (cache != null) {
            if (currentValue == null) {
                response = cache.replace(key, value, getCallerUuid(), completionId);
            } else {
                response = cache.replace(key, currentValue, value, getCallerUuid(), completionId);
            }
        } else {
            response = Boolean.FALSE;
        }
    }

    @Override
    public void afterRun() throws Exception {
        SerializationService ss = getNodeEngine().getSerializationService();
        ss.disposeData(key);

        if (Boolean.FALSE.equals(response)) {
            disposeInternal(ss);
        } else {
            if (currentValue != null) {
                ss.disposeData(currentValue);
            }
        }
        super.afterRun();
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutBackupOperation(name, key, value, expiryPolicy);
    }

    @Override
    protected void disposeInternal(SerializationService binaryService) {
        binaryService.disposeData(value);
        if (currentValue != null) {
            binaryService.disposeData(currentValue);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(value);
        out.writeData(currentValue);
        out.writeObject(expiryPolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = AbstractHiDensityCacheOperation.readOperationData(in);
        currentValue = AbstractHiDensityCacheOperation.readOperationData(in);
        expiryPolicy = in.readObject();
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.REPLACE;
    }
}
