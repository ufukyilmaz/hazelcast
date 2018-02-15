package com.hazelcast.cache.wan;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.EnterpriseCacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.operation.CacheCreateConfigOperation;
import com.hazelcast.cache.operation.EnterpriseCacheOperationProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.MergingEntryHolder;
import com.hazelcast.wan.WanReplicationEvent;

import static com.hazelcast.cache.impl.operation.MutableOperation.IGNORE_COMPLETION;
import static com.hazelcast.spi.impl.merge.MergingHolders.createMergeHolder;
import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * This class handles incoming WAN replication events.
 */
public class CacheReplicationSupportingService implements ReplicationSupportingService {

    private static final String ORIGIN = "ENTERPRISE_WAN";

    private final EnterpriseCacheService cacheService;
    private final NodeEngine nodeEngine;
    private final ProxyService proxyService;

    public CacheReplicationSupportingService(EnterpriseCacheService cacheService) {
        this.cacheService = cacheService;
        this.nodeEngine = cacheService.getNodeEngine();
        this.proxyService = nodeEngine.getProxyService();
    }

    @Override
    public void onReplicationEvent(WanReplicationEvent replicationEvent) {
        Object eventObject = replicationEvent.getEventObject();

        if (eventObject instanceof CacheReplicationObject) {
            CacheReplicationObject cacheReplicationObject = (CacheReplicationObject) eventObject;
            CacheConfig cacheConfig;
            try {
                cacheConfig = getCacheConfig(cacheReplicationObject.getNameWithPrefix(), cacheReplicationObject.getCacheName());
            } catch (Exception e) {
                throw rethrow(e);
            }
            CacheConfig existingCacheConfig = cacheService.putCacheConfigIfAbsent(cacheConfig);
            if (existingCacheConfig == null) {
                CacheCreateConfigOperation op = new CacheCreateConfigOperation(cacheConfig, true);
                // run "CacheCreateConfigOperation" on this node, the operation itself handles interaction with other nodes
                // this operation doesn't block operation thread even "syncCreate" is specified
                // in that case, scheduled thread is used, not the operation thread
                InternalCompletableFuture future = nodeEngine.getOperationService()
                        .invokeOnTarget(CacheService.SERVICE_NAME, op, nodeEngine.getThisAddress());
                future.join();
            }

            // Proxies should be created to initialize listeners, etc. and to show WAN replicated caches in mancenter.
            // Otherwise, users are forced to manually call cacheManager#getCache
            // Fixes https://github.com/hazelcast/hazelcast-enterprise/issues/1049
            proxyService.getDistributedObject(CacheService.SERVICE_NAME, cacheConfig.getNameWithPrefix());

            WanReplicationRef wanReplicationRef = cacheConfig.getWanReplicationRef();
            if (wanReplicationRef != null && wanReplicationRef.isRepublishingEnabled()) {
                cacheService.publishWanEvent(cacheConfig.getNameWithPrefix(), replicationEvent);
            }

            InternalCompletableFuture completableFuture = null;
            if (cacheReplicationObject instanceof CacheReplicationUpdate) {
                completableFuture = handleCacheUpdate((CacheReplicationUpdate) cacheReplicationObject, cacheConfig);
            } else if (cacheReplicationObject instanceof CacheReplicationRemove) {
                completableFuture = handleCacheRemove((CacheReplicationRemove) cacheReplicationObject, cacheConfig);
            }

            if (completableFuture != null
                    && replicationEvent.getAcknowledgeType() == WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE) {
                completableFuture.join();
            }
        }
    }

    private CacheConfig getCacheConfig(String cacheNameWithPrefix, String name) {
        CacheConfig cacheConfig = cacheService.getCacheConfig(cacheNameWithPrefix);
        if (cacheConfig == null) {
            cacheConfig = cacheService.findCacheConfig(name);
            if (cacheConfig != null) {
                cacheConfig.setManagerPrefix(cacheNameWithPrefix.substring(0, cacheNameWithPrefix.lastIndexOf(name)));
            }
        }
        return cacheConfig;
    }

    private InternalCompletableFuture handleCacheRemove(CacheReplicationRemove cacheReplicationRemove,
                                                        CacheConfig cacheConfig) {
        EnterpriseCacheOperationProvider operationProvider;
        operationProvider = (EnterpriseCacheOperationProvider) cacheService
                .getCacheOperationProvider(cacheReplicationRemove.getNameWithPrefix(), cacheConfig.getInMemoryFormat());
        Operation operation = operationProvider.createWanRemoveOperation(ORIGIN, cacheReplicationRemove.getKey(),
                IGNORE_COMPLETION);
        return invokeOnPartition(cacheReplicationRemove.getKey(), operation);
    }

    private InternalCompletableFuture handleCacheUpdate(CacheReplicationUpdate cacheReplicationUpdate, CacheConfig cacheConfig) {
        EnterpriseCacheOperationProvider operationProvider;
        operationProvider = (EnterpriseCacheOperationProvider) cacheService
                .getCacheOperationProvider(cacheReplicationUpdate.getNameWithPrefix(), cacheConfig.getInMemoryFormat());
        Object mergePolicy = cacheService.getCacheMergePolicyProvider().getMergePolicy(cacheReplicationUpdate.getMergePolicy());

        Operation operation;
        if (mergePolicy instanceof SplitBrainMergePolicy) {
            MergingEntryHolder<Data, Data> mergingEntry = createMergeHolder(cacheReplicationUpdate.getEntryView());
            operation = operationProvider.createWanMergeOperation(ORIGIN, mergingEntry, (SplitBrainMergePolicy) mergePolicy,
                    IGNORE_COMPLETION);
        } else {
            CacheEntryView<Data, Data> entryView = cacheReplicationUpdate.getEntryView();
            operation = operationProvider.createLegacyWanMergeOperation(ORIGIN, entryView, (CacheMergePolicy) mergePolicy,
                    IGNORE_COMPLETION);
        }
        return invokeOnPartition(cacheReplicationUpdate.getKey(), operation);
    }

    private InternalCompletableFuture invokeOnPartition(Data key, Operation operation) {
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            return nodeEngine.getOperationService()
                    .invokeOnPartition(ICacheService.SERVICE_NAME, operation, partitionId);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }
}
