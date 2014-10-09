package com.hazelcast.cache.enterprise;

import com.hazelcast.cache.AbstractCacheRecordStore;
import com.hazelcast.cache.enterprise.operation.CacheEvictionOperation;
import com.hazelcast.cache.CacheRecord;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.elasticcollections.map.BinaryOffHeapHashMap;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author mdogan 05/02/14
 */
public final class EnterpriseCacheRecordStore extends AbstractCacheRecordStore {

    final String name;
    final int partitionId;
    final NodeEngine nodeEngine;
    final EnterpriseCacheService cacheService;
    final CacheConfig cacheConfig;

    final ScheduledFuture<?> evictionTaskFuture;
    final Operation evictionOperation;

    EnterpriseCacheRecordStore(final String name, int partitionId, NodeEngine nodeEngine,
            final EnterpriseCacheService cacheService) {
        super(cacheService.getSerializationService(), nodeEngine.getConfig().findCacheConfig(name), 1000);
        this.name = name;
        this.partitionId = partitionId;
        this.nodeEngine = nodeEngine;
        this.cacheService = cacheService;
        cacheConfig = nodeEngine.getConfig().findCacheConfig(name);

        evictionOperation = createEvictionOperation(10);
        evictionTaskFuture = nodeEngine.getExecutionService()
                .scheduleWithFixedDelay("hz:cache", new EvictionTask(), 5, 5, TimeUnit.SECONDS);
    }

    @Override
    protected Callback<Data> createEvictionCallback() {
        return new Callback<Data>() {
            public void notify(Data object) {
                cacheService.sendInvalidationEvent(name, object, "<NA>");
            }
        };
    }

    @Override
    protected void onEntryInvalidated(Data key, String source) {
        cacheService.sendInvalidationEvent(name, key, source);
    }

    protected void onClear() {
        cacheService.sendInvalidationEvent(name, null, "<NA>");
    }

    @Override
    protected void onDestroy() {
        cacheService.sendInvalidationEvent(name, null, "<NA>");
        ScheduledFuture<?> f = evictionTaskFuture;
        if (f != null) {
            f.cancel(true);
        }
    }

    public CacheConfig getConfig() {
        return cacheConfig;
    }

    public BinaryOffHeapHashMap<CacheRecord>.EntryIter iterator(int slot) {
        return map.iterator(slot);
    }

    private class EvictionTask implements Runnable {

        public void run() {
            if (hasTTL()) {
                OperationService operationService = nodeEngine.getOperationService();
                operationService.executeOperation(evictionOperation);
            }
        }
    }

    private Operation createEvictionOperation(int percentage) {
        return new CacheEvictionOperation(name, percentage)
                .setNodeEngine(nodeEngine)
                .setPartitionId(partitionId)
                .setCallerUuid(nodeEngine.getLocalMember().getUuid())
                .setService(cacheService);
    }
}
