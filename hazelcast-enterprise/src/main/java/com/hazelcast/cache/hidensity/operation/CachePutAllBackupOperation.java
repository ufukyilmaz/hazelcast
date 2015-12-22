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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Cache PutAllBackup Operation is the backup operation used by load all operation. Provides backup of
 * multiple entries.
 *
 * @see com.hazelcast.cache.impl.operation.CacheLoadAllOperation
 */
public class CachePutAllBackupOperation
        extends AbstractHiDensityCacheOperation
        implements BackupOperation, MutableOperation, MutatingOperation {

    private Map<Data, Data> cacheRecords;
    private ExpiryPolicy expiryPolicy;

    public CachePutAllBackupOperation() {
    }

    public CachePutAllBackupOperation(String name, Map<Data, Data> cacheRecords, ExpiryPolicy expiryPolicy) {
        super(name);
        this.cacheRecords = cacheRecords;
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    protected void runInternal() throws Exception {
        if (cacheRecords != null) {
            final Iterator<Map.Entry<Data, Data>> iter = cacheRecords.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Data, Data> entry = iter.next();
                Data key = entry.getKey();
                Data value = entry.getValue();
                cache.putBackup(key, value, expiryPolicy);
                iter.remove();
            }
        }
    }

    @Override
    protected void disposeInternal(EnterpriseSerializationService serializationService) {
        if (cacheRecords != null && !cacheRecords.isEmpty()) {
            Iterator<Map.Entry<Data, Data>> iter = cacheRecords.entrySet().iterator();
            // Dispose remaining entries
            while (iter.hasNext()) {
                Map.Entry<Data, Data> entry = iter.next();
                Data key = entry.getKey();
                serializationService.disposeData(key);
                Data value = entry.getValue();
                serializationService.disposeData(value);
                iter.remove();
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(expiryPolicy);

        out.writeInt(cacheRecords != null ? cacheRecords.size() : 0);
        if (cacheRecords != null) {
            for (Map.Entry<Data, Data> entry : cacheRecords.entrySet()) {
                Data key = entry.getKey();
                Data value = entry.getValue();
                out.writeData(key);
                out.writeData(value);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        expiryPolicy = in.readObject();

        final int size = in.readInt();
        if (size > 0) {
            cacheRecords = new HashMap<Data, Data>(size);
            for (int i = 0; i < size; i++) {
                Data key = AbstractHiDensityCacheOperation.readNativeMemoryOperationData(in);
                Data value = AbstractHiDensityCacheOperation.readNativeMemoryOperationData(in);
                cacheRecords.put(key, value);
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
