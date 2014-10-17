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
public class CacheGetAndReplaceOperation extends BackupAwareOffHeapCacheOperation {

    private Data value;

    public CacheGetAndReplaceOperation() {
    }

    public CacheGetAndReplaceOperation(String name, Data key, Data value) {
        super(name, key);
        this.value = value;
    }

    @Override
    public void runInternal() throws Exception {
        response = cache.getAndReplace(key, value, getCallerUuid());
    }

    @Override
    public void afterRun() throws Exception {
        SerializationService ss = getNodeEngine().getSerializationService();
        ss.disposeData(key);

        if (response == null) {
            disposeInternal(ss);
        }
    }

    @Override
    public boolean shouldBackup() {
        return response != null;
    }

    @Override
    public Operation getBackupOperation() {
//        return new CachePutBackupOperation(name, key, value, -1);
        return null;
    }

    @Override
    protected void disposeInternal(SerializationService binaryService) {
        binaryService.disposeData(value);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = readOffHeapData(in);
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.GET_AND_REPLACE;
    }

}
