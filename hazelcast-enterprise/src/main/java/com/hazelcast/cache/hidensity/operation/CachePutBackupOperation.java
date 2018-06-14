package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.BackupOperation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;

/**
 * Creates a backup for a JCache entry.
 */
public class CachePutBackupOperation
        extends KeyBasedHiDensityCacheOperation
        implements BackupOperation, MutableOperation {

    private Data value;
    private ExpiryPolicy expiryPolicy;
    private boolean wanOriginated;

    private transient CacheRecord record;

    public CachePutBackupOperation() {
    }

    public CachePutBackupOperation(String name, Data key, Data value,
                                   ExpiryPolicy expiryPolicy) {
        super(name, key);
        this.value = value;
        this.expiryPolicy = expiryPolicy;
    }

    public CachePutBackupOperation(String name, Data key, Data value,
                                   ExpiryPolicy expiryPolicy, boolean wanOriginated) {
        this(name, key, value, expiryPolicy);
        this.wanOriginated = wanOriginated;
    }

    @Override
    public void runInternal() {
        if (recordStore == null) {
            return;
        }

        HiDensityCacheRecordStore hdCache = (HiDensityCacheRecordStore) recordStore;
        record = hdCache.putBackup(key, value, expiryPolicy);
        response = Boolean.TRUE;
    }

    @Override
    public void afterRun() throws Exception {
        if (!wanOriginated) {
            publishWanUpdate(key, record);
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
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(expiryPolicy);
        out.writeData(value);
        out.writeBoolean(wanOriginated);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        expiryPolicy = in.readObject();
        value = readNativeMemoryOperationData(in);
        wanOriginated = in.readBoolean();
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.PUT_BACKUP;
    }
}
