package com.hazelcast.cache.enterprise.impl.onheap;

import com.hazelcast.cache.enterprise.EnterpriseCacheRecordStore;
import com.hazelcast.cache.enterprise.EnterpriseCacheService;
import com.hazelcast.cache.impl.CacheEntryProcessorEntry;
import com.hazelcast.cache.impl.CacheRecordStore;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.cache.impl.record.CacheRecordFactory;
import com.hazelcast.cache.impl.record.CacheRecordMap;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.NodeEngine;

/**
 * @author sozal 14/10/14
 */
public class EnterpriseOnHeapCacheRecordStore
        extends CacheRecordStore
        implements EnterpriseCacheRecordStore {

    public EnterpriseOnHeapCacheRecordStore(final String name,
                                            final int partitionId,
                                            final NodeEngine nodeEngine,
                                            final EnterpriseCacheService cacheService) {
        super(name, partitionId, nodeEngine, cacheService);
    }

    @Override
    protected CacheRecordMap createRecordCacheMap() {
        return new EnterpriseOnHeapCacheRecordMap<Data, EnterpriseOnHeapCacheRecord>(1000);
    }

    @Override
    protected CacheRecordFactory createCacheRecordFactory(InMemoryFormat inMemoryFormat,
                                                          SerializationService serializationService) {
        return new EnterpriseOnHeapCacheRecordFactory(inMemoryFormat, serializationService);
    }

    @Override
    protected CacheEntryProcessorEntry createCacheEntryProcessorEntry(Data key,
                                                                      CacheRecord record,
                                                                      CacheRecordStore cacheRecordStore,
                                                                      long now) {
        return new EnterpriseOnHeapCacheEntryProcessorEntry(key, record, this, now);
    }

    @Override
    public int forceEvict() {
        return super.evictExpiredRecords();
    }

}
