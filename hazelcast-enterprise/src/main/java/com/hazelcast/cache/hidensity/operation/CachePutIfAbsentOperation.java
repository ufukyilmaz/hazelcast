package com.hazelcast.cache.hidensity.operation;

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
 * Creates a JCache entry via {@code putIfAbsent()}.
 */
public class CachePutIfAbsentOperation
        extends BackupAwareKeyBasedHiDensityCacheOperation
        implements MutableOperation, MutatingOperation {

    private Data value;
    private ExpiryPolicy expiryPolicy;

    public CachePutIfAbsentOperation() {
    }

    public CachePutIfAbsentOperation(String name, Data key, Data value,
                                     ExpiryPolicy expiryPolicy) {
        super(name, key);
        this.value = value;
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    protected void runInternal() {
        response = recordStore.putIfAbsent(key, value, expiryPolicy, getCallerUuid(), completionId);
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
        if (!Boolean.TRUE.equals(response)) {
            serializationService.disposeData(key);
            serializationService.disposeData(value);
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
        out.writeObject(expiryPolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = readNativeMemoryOperationData(in);
        expiryPolicy = in.readObject();
    }

    @Override
    public int getClassId() {
        return HiDensityCacheDataSerializerHook.PUT_IF_ABSENT;
    }
}
