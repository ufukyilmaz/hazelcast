package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.impl.CacheClearResponse;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.Operation;

import javax.cache.CacheException;
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
public class CacheLoadAllOperation extends BackupAwareHiDensityCacheOperation {

    private Set<Data> keys;
    private boolean replaceExistingValues;
    private boolean shouldBackup;
    private transient Map<Data, CacheRecord> backupRecords;
    private transient ICacheRecordStore cache;

    public CacheLoadAllOperation() {
    }

    public CacheLoadAllOperation(String name, Set<Data> keys, boolean replaceExistingValues) {
        super(name);
        this.keys = keys;
        this.replaceExistingValues = replaceExistingValues;
    }

    @Override
    public void runInternal() throws Exception {
        final int partitionId = getPartitionId();
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
            final CacheService service = getService();
            cache = service.getOrCreateRecordStore(name, partitionId);
            final Set<Data> keysLoaded = cache.loadAll(filteredKeys, replaceExistingValues);
            shouldBackup = !keysLoaded.isEmpty();
            if (shouldBackup) {
                backupRecords = new HashMap<Data, CacheRecord>();
                for (Data key : keysLoaded) {
                    backupRecords.put(key, cache.getRecord(key));
                }
            }
        } catch (CacheException e) {
            response = new CacheClearResponse(e);
        }
    }

    @Override
    protected void disposeInternal(SerializationService binaryService) {
    }

    @Override
    public boolean shouldBackup() {
        return shouldBackup;
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutAllBackupOperation(name, backupRecords);
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.LOAD_ALL;
    }
}
