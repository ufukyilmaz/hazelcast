package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;

/**
 * Replaces the value of a JCache entry.
 */
public class CacheReplaceOperation
        extends BackupAwareKeyBasedHiDensityCacheOperation
        implements MutableOperation, MutatingOperation {

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
    protected void runInternal() {
        HiDensityCacheRecordStore hdCache = (HiDensityCacheRecordStore) this.recordStore;
        if (currentValue == null) {
            response = hdCache.replace(key, value, getCallerUuid(), completionId);
        } else {
            response = hdCache.replace(key, currentValue, value, getCallerUuid(), completionId);
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (Boolean.TRUE.equals(response)) {
            publishWanUpdate(key, recordStore.getRecord(key));
        }

        super.afterRun();
        dispose();
    }

    @Override
    protected void disposeInternal(EnterpriseSerializationService serializationService) {
        serializationService.disposeData(key);
        if (!Boolean.TRUE.equals(response)) {
            serializationService.disposeData(value);
        }
        if (currentValue != null) {
            serializationService.disposeData(currentValue);
        }
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response) && recordStore.getRecord(key) != null;
    }

    @Override
    public Operation getBackupOperation() {
        CacheRecord record = recordStore.getRecord(key);
        return new CachePutBackupOperation(name, key, value, expiryPolicy, record.getCreationTime());
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
        value = readNativeMemoryOperationData(in);
        currentValue = readNativeMemoryOperationData(in);
        expiryPolicy = in.readObject();
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.REPLACE;
    }

}
