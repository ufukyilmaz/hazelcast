package com.hazelcast.cache;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.ReadonlyOperation;

/**
 * @author mdogan 05/02/14
 */
public class CacheGetOperation extends AbstractOffHeapCacheOperation implements ReadonlyOperation {


    public CacheGetOperation() {
    }

    public CacheGetOperation(String name, Data key) {
        super(name, key);
    }

    @Override
    public void runInternal() throws Exception {
        response = cache != null ? cache.get(key) : null;
    }

    @Override
    public void afterRun() throws Exception {
        dispose();
    }

    @Override
    protected void disposeInternal(SerializationService binaryService) {
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.GET;
    }
}
