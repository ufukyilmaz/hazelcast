package com.hazelcast.cache.impl.hidensity.operation;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;

/**
 * Determines if this store contains an entry for the specified key.
 */
public class CacheContainsKeyOperation
        extends KeyBasedHiDensityCacheOperation
        implements ReadonlyOperation {

    public CacheContainsKeyOperation() {
    }

    public CacheContainsKeyOperation(String name, Data key) {
        super(name, key, true);
    }

    @Override
    protected void runInternal() {
        response = recordStore != null && recordStore.contains(key);
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();
        dispose();
    }

    @Override
    protected void disposeInternal(EnterpriseSerializationService serializationService) {
        serializationService.disposeData(key);
    }

    @Override
    public int getClassId() {
        return HiDensityCacheDataSerializerHook.CONTAINS_KEY;
    }
}
