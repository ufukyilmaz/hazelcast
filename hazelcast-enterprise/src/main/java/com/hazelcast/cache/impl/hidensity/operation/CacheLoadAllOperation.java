package com.hazelcast.cache.impl.hidensity.operation;

import com.hazelcast.cache.impl.hidensity.HiDensityCacheRecord;
import com.hazelcast.cache.impl.CacheClearResponse;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.internal.partition.IPartitionService;

import javax.cache.CacheException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Loads all entries of the keys to partition record store {@link com.hazelcast.cache.impl.ICacheRecordStore}.
 * <p>{@link com.hazelcast.cache.impl.operation.CacheLoadAllOperationFactory} creates this operation.</p>
 * <p>Functionality: Filters out the partition keys and calls
 * {@link com.hazelcast.cache.impl.ICacheRecordStore#loadAll(java.util.Set keys, boolean replaceExistingValues)}.</p>
 */
public class CacheLoadAllOperation
        extends BackupAwareKeyBasedHiDensityCacheOperation implements MutatingOperation {

    private Set<Data> keys;
    private boolean replaceExistingValues;
    private boolean shouldBackup;

    private transient CacheBackupRecordStore cacheBackupRecordStore;

    public CacheLoadAllOperation() {
    }

    public CacheLoadAllOperation(String name, Set<Data> keys, boolean replaceExistingValues) {
        super(name);
        this.keys = keys;
        this.replaceExistingValues = replaceExistingValues;
    }

    @Override
    protected void runInternal() {
        final IPartitionService partitionService = getNodeEngine().getPartitionService();

        Set<Data> filteredKeys = new HashSet<Data>();
        if (keys != null) {
            for (Data k : keys) {
                if (partitionService.getPartitionId(k) == getPartitionId()) {
                    filteredKeys.add(k);
                }
            }
        }
        if (filteredKeys.isEmpty()) {
            return;
        }
        try {
            final Set<Data> keysLoaded = recordStore.loadAll(filteredKeys, replaceExistingValues);
            shouldBackup = !keysLoaded.isEmpty();
            if (shouldBackup) {
                cacheBackupRecordStore = new CacheBackupRecordStore();
                for (Data key : keysLoaded) {
                    HiDensityCacheRecord record = (HiDensityCacheRecord) recordStore.getRecord(key);
                    // Loaded keys may have been evicted, then record will be null.
                    // So if the loaded key is evicted, don't send it to backup.
                    if (record != null) {
                        cacheBackupRecordStore.addBackupRecord(key, record.getValue(), record.getCreationTime());
                    }
                }
            }
        } catch (CacheException e) {
            response = new CacheClearResponse(e);
        }
    }

    @Override
    public boolean shouldBackup() {
        return shouldBackup;
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutAllBackupOperation(name, cacheBackupRecordStore, null);
    }

    // Currently, since `CacheLoadAllOperation` is created by `CacheLoadAllOperationFactory`,
    // it is not sent over network and not serialized.
    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(replaceExistingValues);
        if (keys == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(keys.size());
            for (Data key : keys) {
                IOUtil.writeData(out, key);
            }
        }
    }

    // Currently, since `CacheLoadAllOperation` is created by `CacheLoadAllOperationFactory`,
    // it is not received over network and not deserialized.
    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        replaceExistingValues = in.readBoolean();
        int size = in.readInt();
        keys = new HashSet<Data>(size);
        for (int i = 0; i < size; i++) {
            Data key = HiDensityCacheOperation.readHeapOperationData(in);
            keys.add(key);
        }
    }

    @Override
    public int getClassId() {
        return HiDensityCacheDataSerializerHook.LOAD_ALL;
    }

}
