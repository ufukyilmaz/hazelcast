package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.MutatingOperation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Cache PutAllBackup Operation is the backup operation used by load all operation. Provides backup of
 * multiple entries.
 *
 * @see com.hazelcast.cache.impl.operation.CacheLoadAllOperation
 */
public class CachePutAllBackupOperation
        extends AbstractHiDensityCacheOperation
        implements BackupOperation, MutableOperation, MutatingOperation {

    private CacheBackupRecordStore cacheBackupRecordStore;
    private ExpiryPolicy expiryPolicy;

    public CachePutAllBackupOperation() {
    }

    public CachePutAllBackupOperation(String name, CacheBackupRecordStore cacheBackupRecordStore,
                                      ExpiryPolicy expiryPolicy) {
        super(name);
        this.cacheBackupRecordStore = cacheBackupRecordStore;
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    protected void runInternal() throws Exception {
        if (cacheBackupRecordStore != null) {
            List<CacheBackupRecordStore.CacheBackupRecord> cacheBackupRecords = cacheBackupRecordStore.backupRecords;
            Iterator<CacheBackupRecordStore.CacheBackupRecord> iter = cacheBackupRecords.iterator();
            while (iter.hasNext()) {
                CacheBackupRecordStore.CacheBackupRecord cacheBackupRecord = iter.next();
                cache.putBackup(cacheBackupRecord.key, cacheBackupRecord.value, expiryPolicy);
                iter.remove();
            }
        }
    }

    @Override
    protected void disposeInternal(EnterpriseSerializationService serializationService) {
        if (cacheBackupRecordStore != null) {
            List<CacheBackupRecordStore.CacheBackupRecord> cacheBackupRecords = cacheBackupRecordStore.backupRecords;
            Iterator<CacheBackupRecordStore.CacheBackupRecord> iter = cacheBackupRecords.iterator();
            // Dispose remaining entries
            while (iter.hasNext()) {
                CacheBackupRecordStore.CacheBackupRecord cacheBackupRecord = iter.next();
                serializationService.disposeData(cacheBackupRecord.key);
                serializationService.disposeData(cacheBackupRecord.value);
                iter.remove();
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(expiryPolicy);
        List<CacheBackupRecordStore.CacheBackupRecord> cacheBackupRecords = null;
        if (cacheBackupRecordStore != null) {
            cacheBackupRecords = cacheBackupRecordStore.backupRecords;
        }
        out.writeInt(cacheBackupRecords != null ? cacheBackupRecords.size() : 0);
        if (cacheBackupRecords != null) {
            Iterator<CacheBackupRecordStore.CacheBackupRecord> iter = cacheBackupRecords.iterator();
            while (iter.hasNext()) {
                CacheBackupRecordStore.CacheBackupRecord cacheBackupRecord = iter.next();
                out.writeData(cacheBackupRecord.key);
                out.writeData(cacheBackupRecord.value);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        expiryPolicy = in.readObject();

        final int size = in.readInt();
        if (size > 0) {
            cacheBackupRecordStore = new CacheBackupRecordStore(size);
            for (int i = 0; i < size; i++) {
                Data key = AbstractHiDensityCacheOperation.readNativeMemoryOperationData(in);
                Data value = AbstractHiDensityCacheOperation.readNativeMemoryOperationData(in);
                cacheBackupRecordStore.addBackupRecord(key, value);
            }
        }
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.PUT_ALL_BACKUP;
    }

    @Override
    public int getFactoryId() {
        return HiDensityCacheDataSerializerHook.F_ID;
    }

}
