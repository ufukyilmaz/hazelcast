package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.event.CacheWanEventPublisher;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.cache.impl.CacheEntryViews.createDefaultEntryView;
import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * Transfers merged data to backup replicas
 */
public class CacheMergeBackupOperation extends AbstractHiDensityCacheOperation
        implements BackupOperation, MutableOperation {

    private Map<Data, CacheRecord> cacheRecords;

    public CacheMergeBackupOperation() {
    }

    public CacheMergeBackupOperation(String cacheNameWithPrefix, Map<Data, CacheRecord> cacheRecords) {
        super(cacheNameWithPrefix);
        this.cacheRecords = cacheRecords;
    }

    @Override
    protected void runInternal() {
        if (cache == null) {
            return;
        }
        if (cacheRecords != null) {
            for (Map.Entry<Data, CacheRecord> entry : cacheRecords.entrySet()) {
                CacheRecord record = entry.getValue();
                cache.putRecord(entry.getKey(), record, true);

                publishWanEvent(entry.getKey(), record);
            }
        }
    }

    private void publishWanEvent(Data key, CacheRecord record) {
        if (cache.isWanReplicationEnabled()) {
            ICacheService service = getService();
            CacheWanEventPublisher publisher = service.getCacheWanEventPublisher();
            CacheEntryView<Data, Data> view = createDefaultEntryView(key, toHeapData(record.getValue()), record);
            publisher.publishWanReplicationUpdateBackup(name, view);
        }
    }

    private Data toHeapData(Object o) {
        return ((InternalSerializationService) getNodeEngine().getSerializationService()).toData(o, DataType.HEAP);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(cacheRecords != null);
        if (cacheRecords != null) {
            out.writeInt(cacheRecords.size());
            for (Map.Entry<Data, CacheRecord> entry : cacheRecords.entrySet()) {
                Data key = entry.getKey();
                CacheRecord record = entry.getValue();
                out.writeData(key);
                out.writeObject(record);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        boolean recordNotNull = in.readBoolean();
        if (recordNotNull) {
            int size = in.readInt();
            cacheRecords = createHashMap(size);
            for (int i = 0; i < size; i++) {
                Data key = in.readData();
                CacheRecord record = in.readObject();
                cacheRecords.put(key, record);
            }
        }
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.MERGE_BACKUP;
    }
}
