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
 * Associates the specified value with the specified key in this cache, returning an existing value if one existed.
 */
public class CachePutOperation
        extends BackupAwareKeyBasedHiDensityCacheOperation
        implements MutableOperation, MutatingOperation {

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
    protected void runInternal() {
        if (get) {
            response = recordStore.getAndPut(key, value, expiryPolicy, getCallerUuid(), completionId);
        } else {
            recordStore.put(key, value, expiryPolicy, getCallerUuid(), completionId);
            response = null;
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (recordStore.isWanReplicationEnabled()) {
            publishWanUpdate(key, recordStore.getRecord(key));
        }
        super.afterRun();
    }

    @Override
    protected void disposeInternal(EnterpriseSerializationService serializationService) {
        // If run is completed successfully, don't dispose key and value since they are handled in the record store.
        // Although run is completed successfully there may be still error (while sending response, ...), in this case,
        // unused data (such as key on update) is handled (disposed) through `dispose()` > `disposeDeferredBlocks()`.
        if (!runCompleted) {
            serializationService.disposeData(key);
            serializationService.disposeData(value);
        }
    }

    @Override
    public boolean shouldBackup() {
        return recordStore.getRecord(key) != null;
    }

    @Override
    public Operation getBackupOperation() {
        CacheRecord record = recordStore.getRecord(key);
        return new CachePutBackupOperation(name, key, value, expiryPolicy, record.getCreationTime());
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
        value = readNativeMemoryOperationData(in);
    }

    @Override
    public int getClassId() {
        return HiDensityCacheDataSerializerHook.PUT;
    }
}
