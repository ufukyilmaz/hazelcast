package com.hazelcast.cache.enterprise.operation;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.enterprise.BreakoutCacheRecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.Operation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;

/**
 * @author mdogan 05/02/14
 */
public class CachePutIfAbsentOperation extends BackupAwareBreakoutCacheOperation {

    private Data value;
    private ExpiryPolicy expiryPolicy;

    public CachePutIfAbsentOperation() {
    }

    public CachePutIfAbsentOperation(String name, Data key, Data value,
                                     ExpiryPolicy expiryPolicy) {
        super(name, key);
        this.value = value;
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    public void runInternal() throws Exception {
        EnterpriseCacheService service = getService();
        BreakoutCacheRecordStore cache =
                (BreakoutCacheRecordStore) service.getOrCreateCache(name, getPartitionId());
        response = cache.putIfAbsent(key, value, expiryPolicy, getCallerUuid());
    }

    @Override
    public void afterRun() throws Exception {
        if (Boolean.FALSE.equals(response)) {
            dispose();
        }
        super.afterRun();
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutBackupOperation(name, key, value, expiryPolicy);
    }

    @Override
    protected void disposeInternal(SerializationService binaryService) {
        binaryService.disposeData(value);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(value);
        out.writeObject(expiryPolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = readNativeData(in);
        expiryPolicy = in.readObject();
    }

    @Override
    public int getId() {
        return EnterpriseCacheDataSerializerHook.PUT_IF_ABSENT;
    }
}
