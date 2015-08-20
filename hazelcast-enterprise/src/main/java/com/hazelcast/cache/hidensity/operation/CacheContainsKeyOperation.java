package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.ReadonlyOperation;

/**
 * @author mdogan 05/02/14
 */
public class CacheContainsKeyOperation
        extends AbstractKeyBasedHiDensityCacheOperation
        implements ReadonlyOperation {

    public CacheContainsKeyOperation() {
    }

    public CacheContainsKeyOperation(String name, Data key) {
        super(name, key, true);
    }

    @Override
    protected void runInternal() throws Exception {
        response = cache != null ? cache.contains(key) : false;
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
    public int getId() {
        return HiDensityCacheDataSerializerHook.CONTAINS_KEY;
    }

}
