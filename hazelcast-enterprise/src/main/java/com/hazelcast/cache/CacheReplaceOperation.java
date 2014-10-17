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
public class CacheReplaceOperation extends BackupAwareOffHeapCacheOperation {

    private Data value;
    private Data currentValue; // replace if same

    public CacheReplaceOperation() {
    }

    public CacheReplaceOperation(String name, Data key, Data value) {
        super(name, key);
        this.value = value;
    }

    public CacheReplaceOperation(String name, Data key, Data oldValue, Data newValue) {
        super(name, key);
        this.value = newValue;
        this.currentValue = oldValue;
    }

    @Override
    public void runInternal() throws Exception {
        if (cache != null) {
            if (currentValue == null) {
                response = cache.replace(key, value, getCallerUuid());
            } else {
                response = cache.replace(key, currentValue, value, getCallerUuid());
            }
        } else {
            response = Boolean.FALSE;
        }
    }

    @Override
    public void afterRun() throws Exception {
        SerializationService ss = getNodeEngine().getSerializationService();
        ss.disposeData(key);

        if (response == Boolean.FALSE) {
            disposeInternal(ss);
        } else {
            if (currentValue != null) {
                ss.disposeData(currentValue);
            }
        }
    }

    @Override
    public boolean shouldBackup() {
        return response == Boolean.TRUE;
    }

    @Override
    public Operation getBackupOperation() {
//        return new CachePutBackupOperation(name, key, value, -1);
        return null;
    }

    @Override
    protected void disposeInternal(SerializationService binaryService) {
        binaryService.disposeData(value);
        if (currentValue != null) {
            binaryService.disposeData(currentValue);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(value);
        out.writeData(currentValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = readOffHeapData(in);
        currentValue = readOffHeapData(in);
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.REPLACE;
    }

}
