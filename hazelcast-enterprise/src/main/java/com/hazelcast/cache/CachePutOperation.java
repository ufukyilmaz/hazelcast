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
public class CachePutOperation extends BackupAwareOffHeapCacheOperation {

    private Data value;
    private boolean get; // getAndPut
    private long ttlMillis;

    public CachePutOperation() {
    }

    public CachePutOperation(String name, Data key, Data value, long ttlMillis) {
        super(name, key);
        this.value = value;
        this.ttlMillis = ttlMillis;
        get = false;
    }

    public CachePutOperation(String name, Data key, Data value, long ttlMillis, boolean get) {
        super(name, key);
        this.value = value;
        this.ttlMillis = ttlMillis;
        this.get = get;
    }

    @Override
    public void runInternal() throws Exception {
        if (get) {
            response = cache.getAndPut(key, value, ttlMillis, getCallerUuid());
        } else {
            cache.put(key, value, ttlMillis, getCallerUuid());
            response = null;
        }
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutBackupOperation(name, key, value, ttlMillis);
    }

    @Override
    protected void disposeInternal(SerializationService binaryService) {
        binaryService.disposeData(value);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(get);
        out.writeLong(ttlMillis);
        out.writeData(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        get = in.readBoolean();
        ttlMillis = in.readLong();
        value = readOffHeapData(in);
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.PUT;
    }

}
