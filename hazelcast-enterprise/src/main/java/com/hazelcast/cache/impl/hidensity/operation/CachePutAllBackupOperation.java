package com.hazelcast.cache.impl.hidensity.operation;

import com.hazelcast.cache.impl.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

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
public class CachePutAllBackupOperation extends HiDensityCacheOperation
        implements BackupOperation, MutableOperation {

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
    protected void runInternal() {
        if (recordStore == null) {
            return;
        }

        List<CacheBackupRecordStore.CacheBackupRecord> cacheBackupRecords = cacheBackupRecordStore.backupRecords;

        HiDensityCacheRecordStore hdCache = (HiDensityCacheRecordStore) recordStore;
        Iterator<CacheBackupRecordStore.CacheBackupRecord> iter = cacheBackupRecords.iterator();
        while (iter.hasNext()) {
            CacheBackupRecordStore.CacheBackupRecord cacheBackupRecord = iter.next();
            CacheRecord record = hdCache.putBackup(cacheBackupRecord.key, cacheBackupRecord.value,
                    cacheBackupRecord.creationTime, expiryPolicy);
            publishWanUpdate(cacheBackupRecord.key, record);
            iter.remove();
        }
    }

    @Override
    protected void disposeInternal(EnterpriseSerializationService serializationService) {
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

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(expiryPolicy);
        List<CacheBackupRecordStore.CacheBackupRecord> cacheBackupRecords = cacheBackupRecordStore.backupRecords;
        out.writeInt(cacheBackupRecords != null ? cacheBackupRecords.size() : 0);
        if (cacheBackupRecords != null) {
            for (CacheBackupRecordStore.CacheBackupRecord cacheBackupRecord : cacheBackupRecords) {
                IOUtil.writeData(out, cacheBackupRecord.key);
                IOUtil.writeData(out, cacheBackupRecord.value);
                out.writeLong(cacheBackupRecord.creationTime);
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
                Data key = HiDensityCacheOperation.readNativeMemoryOperationData(in);
                Data value = HiDensityCacheOperation.readNativeMemoryOperationData(in);
                long creationTime = in.readLong();
                cacheBackupRecordStore.addBackupRecord(key, value, creationTime);
            }
        }
    }

    @Override
    public int getClassId() {
        return HiDensityCacheDataSerializerHook.PUT_ALL_BACKUP;
    }

    @Override
    public int getFactoryId() {
        return HiDensityCacheDataSerializerHook.F_ID;
    }

}
