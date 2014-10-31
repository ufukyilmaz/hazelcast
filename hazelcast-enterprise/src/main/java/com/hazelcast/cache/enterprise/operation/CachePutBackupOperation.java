package com.hazelcast.cache.enterprise.operation;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.enterprise.BreakoutCacheRecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.BackupOperation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;

/**
 * @author mdogan 05/02/14
 */
public class CachePutBackupOperation
        extends AbstractBreakoutCacheOperation
        implements BackupOperation {

    private Data value;
    private ExpiryPolicy expiryPolicy;

    public CachePutBackupOperation() {
    }

    public CachePutBackupOperation(String name, Data key, Data value,
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
        cache.put(key, value, expiryPolicy, null);
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
        out.writeObject(expiryPolicy);
        out.writeData(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        expiryPolicy = in.readObject();
        value = readNativeData(in);
    }

    @Override
    public int getId() {
        return EnterpriseCacheDataSerializerHook.PUT_BACKUP;
    }
}
