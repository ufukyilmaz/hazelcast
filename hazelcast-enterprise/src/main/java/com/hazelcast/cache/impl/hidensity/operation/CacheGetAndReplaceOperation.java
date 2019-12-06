package com.hazelcast.cache.impl.hidensity.operation;

import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;

/**
 * Atomically replaces the value for a given key if and only if there is a value currently mapped by the key.
 */
public class CacheGetAndReplaceOperation
        extends BackupAwareKeyBasedHiDensityCacheOperation
        implements MutableOperation, MutatingOperation {

    private Data value;
    private ExpiryPolicy expiryPolicy;

    public CacheGetAndReplaceOperation() {
    }

    public CacheGetAndReplaceOperation(String name, Data key, Data value,
                                       ExpiryPolicy expiryPolicy) {
        super(name, key);
        this.value = value;
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    protected void runInternal() {
        response = recordStore.getAndReplace(key, value, expiryPolicy, getCallerUuid(), completionId);
    }

    @Override
    public void afterRun() throws Exception {
        if (response != null) {
            publishWanUpdate(key, recordStore.getRecord(key));
        }
        super.afterRun();
        dispose();
    }

    @Override
    protected void disposeInternal(EnterpriseSerializationService serializationService) {
        serializationService.disposeData(key);
        if (response == null) {
            serializationService.disposeData(value);
        }
    }

    @Override
    public boolean shouldBackup() {
        return response != null && recordStore.getRecord(key) != null;
    }

    @Override
    public Operation getBackupOperation() {
        CacheRecord record = recordStore.getRecord(key);
        return new CachePutBackupOperation(name, key, value, expiryPolicy, record.getCreationTime());
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeData(out, value);
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
        return HiDensityCacheDataSerializerHook.GET_AND_REPLACE;
    }

}
