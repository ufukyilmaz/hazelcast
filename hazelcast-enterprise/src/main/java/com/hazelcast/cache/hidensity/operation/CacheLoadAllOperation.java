package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.impl.CacheClearResponse;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.Operation;

import javax.cache.CacheException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Loads all entries of the keys to partition record store {@link com.hazelcast.cache.impl.ICacheRecordStore}.
 * <p>{@link com.hazelcast.cache.impl.operation.CacheLoadAllOperationFactory} creates this operation.</p>
 * <p>Functionality: Filters out the partition keys and calls
 * {@link com.hazelcast.cache.impl.ICacheRecordStore#loadAll(java.util.Set keys, boolean replaceExistingValues)}.</p>
 */
public class CacheLoadAllOperation
        extends BackupAwareKeyBasedHiDensityCacheOperation {

    private Set<Data> keys;
    private boolean replaceExistingValues;
    private boolean shouldBackup;

    private transient Map<Data, CacheRecord> backupRecords;

    public CacheLoadAllOperation() {
    }

    public CacheLoadAllOperation(String name, Set<Data> keys, boolean replaceExistingValues) {
        super(name);
        this.keys = keys;
        this.replaceExistingValues = replaceExistingValues;
    }

    @Override
    protected void runInternal() throws Exception {
        final InternalPartitionService partitionService = getNodeEngine().getPartitionService();

        Set<Data> filteredKeys = new HashSet<Data>();
        if (keys != null) {
            for (Data k : keys) {
                if (partitionService.getPartitionId(k) == partitionId) {
                    filteredKeys.add(k);
                }
            }
        }
        if (filteredKeys.isEmpty()) {
            return;
        }
        try {
            final Set<Data> keysLoaded = cache.loadAll(filteredKeys, replaceExistingValues);
            shouldBackup = !keysLoaded.isEmpty();
            if (shouldBackup) {
                backupRecords = new HashMap<Data, CacheRecord>();
                for (Data key : keysLoaded) {
                    CacheRecord record = cache.getRecord(key);
                    // Loaded keys may have been evicted, then record will be null.
                    // So if the loaded key is evicted, don't send it to backup.
                    if (record != null) {
                        backupRecords.put(key, record);
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
        return new CachePutAllBackupOperation(name, backupRecords);
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
                out.writeData(key);
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
            Data key = AbstractHiDensityCacheOperation.readHeapOperationData(in);
            keys.add(key);
        }
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.LOAD_ALL;
    }

}
