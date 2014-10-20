package com.hazelcast.cache;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @author mdogan 05/02/14
 */
public class CacheRemoveOperation extends BackupAwareOffHeapCacheOperation {

    private Data currentValue; // if same

    public CacheRemoveOperation() {
    }

    public CacheRemoveOperation(String name, Data key) {
        super(name, key);
    }

    public CacheRemoveOperation(String name, Data key, Data currentValue) {
        super(name, key);
        this.currentValue = currentValue;
    }

    @Override
    public void runInternal() throws Exception {
        if (cache != null) {
            if (currentValue == null) {
                response = cache.remove(key, getCallerUuid());
            } else {
                response = cache.remove(key, currentValue, getCallerUuid());
            }
        } else {
            response = Boolean.FALSE;
        }
    }

    @Override
    public void afterRun() throws Exception {
        dispose();
    }

    @Override
    public boolean shouldBackup() {
        return response == Boolean.TRUE;
    }

    @Override
    public Operation getBackupOperation() {
        return new CacheRemoveBackupOperation(name, key);
    }

    @Override
    protected void disposeInternal(SerializationService binaryService) {
        if (currentValue != null) {
            binaryService.disposeData(currentValue);
        }
    }

    @Override
    public int getId() {
        return EnterpriseCacheDataSerializerHook.REMOVE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(currentValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        currentValue = readOffHeapData(in);
    }
}
