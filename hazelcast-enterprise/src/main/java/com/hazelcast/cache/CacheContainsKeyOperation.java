package com.hazelcast.cache;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.ReadonlyOperation;

/**
 * @author mdogan 05/02/14
 */
public class CacheContainsKeyOperation extends AbstractOffHeapCacheOperation implements ReadonlyOperation {

    public CacheContainsKeyOperation() {
    }

    public CacheContainsKeyOperation(String name, Data key) {
        super(name, key);
    }

    @Override
    public void runInternal() throws Exception {
        response = cache != null && cache.contains(key);
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
        return CacheDataSerializerHook.CONTAINS_KEY;
    }
}
