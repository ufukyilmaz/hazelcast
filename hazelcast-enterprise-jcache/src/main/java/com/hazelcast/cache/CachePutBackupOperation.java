package com.hazelcast.cache;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

/**
 * @author mdogan 05/02/14
 */
public class CachePutBackupOperation extends AbstractCacheOperation implements BackupOperation {

    private Data value;
    private long ttlMillis;

    public CachePutBackupOperation() {
    }

    public CachePutBackupOperation(String name, Data key, Data value, long ttlMillis) {
        super(name, key);
        this.value = value;
        this.ttlMillis = ttlMillis;
    }

    @Override
    public void runInternal() throws Exception {
        CacheService service = getService();
        CacheRecordStore cache = service.getOrCreateCache(name, getPartitionId());
        cache.put(key, value, ttlMillis, null);
        response = Boolean.TRUE;
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    protected void disposeInternal(SerializationService binaryService) {
        binaryService.disposeData(value);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(ttlMillis);
        out.writeData(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        ttlMillis = in.readLong();
        value = readOffHeapData(in);
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.PUT_BACKUP;
    }

}
