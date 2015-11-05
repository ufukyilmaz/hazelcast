package com.hazelcast.cache.wan;

import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.operation.CacheCreateConfigOperation;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.cache.operation.EnterpriseCacheOperationProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.wan.WanReplicationEvent;

import javax.cache.CacheException;

/**
 * This class handles incoming WAN replication events
 */
public class CacheReplicationSupportingService implements ReplicationSupportingService {

    /**
     * Event origin
     */
    public static final String ORIGIN = "ENTERPRISE_WAN";

    private final EnterpriseCacheService cacheService;
    private final NodeEngine nodeEngine;

    public CacheReplicationSupportingService(EnterpriseCacheService cacheService) {
        this.cacheService = cacheService;
        this.nodeEngine = cacheService.getNodeEngine();
    }

    @Override
    public void onReplicationEvent(WanReplicationEvent replicationEvent) {
        Object eventObject = replicationEvent.getEventObject();

        if (eventObject instanceof CacheReplicationObject) {
            CacheReplicationObject cacheReplicationObject = (CacheReplicationObject) eventObject;
            CacheConfig cacheConfig = null;
            try {
                cacheConfig = getCacheConfig(cacheReplicationObject.getNameWithPrefix(),
                                             cacheReplicationObject.getCacheName());
            } catch (Exception e) {
                ExceptionUtil.rethrow(e);
            }
            CacheConfig existingCacheConfig = cacheService.putCacheConfigIfAbsent(cacheConfig);
            if (existingCacheConfig == null) {
                CacheCreateConfigOperation op =
                        new CacheCreateConfigOperation(cacheConfig, true);
                // Run "CacheCreateConfigOperation" on this node. Its itself handles interaction with other nodes.
                // This operation doesn't block operation thread even "syncCreate" is specified.
                // In that case, scheduled thread is used, not operation thread.
                InternalCompletableFuture future =
                        nodeEngine.getOperationService()
                                .invokeOnTarget(CacheService.SERVICE_NAME, op, nodeEngine.getThisAddress());
                future.getSafely();
            }

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
                completableFuture.getSafely();
            }
        }
    }

    private CacheConfig getCacheConfig(String cacheNameWithPrefix, String name) {
        CacheConfig cacheConfig = cacheService.getCacheConfig(cacheNameWithPrefix);
        if (cacheConfig == null) {
            CacheSimpleConfig simpleConfig = cacheService.findCacheConfig(name);
            if (simpleConfig != null) {
                try {
                    cacheConfig = new CacheConfig(simpleConfig);
                    cacheConfig.setName(name);
                    cacheConfig.setManagerPrefix(cacheNameWithPrefix.substring(0, cacheNameWithPrefix.lastIndexOf(name)));
                } catch (Exception e) {
                    //Cannot create the actual config from the declarative one
                    throw new CacheException(e);
                }
            }
        }
        return cacheConfig;
    }

    private InternalCompletableFuture handleCacheRemove(CacheReplicationRemove cacheReplicationRemove,
                                   CacheConfig cacheConfig) {
        EnterpriseCacheOperationProvider operationProvider;
        operationProvider = (EnterpriseCacheOperationProvider) cacheService
                                .getCacheOperationProvider(cacheReplicationRemove.getNameWithPrefix(),
                                                           cacheConfig.getInMemoryFormat());
        Operation operation =
                operationProvider.createWanRemoveOperation(ORIGIN, cacheReplicationRemove.getKey(),
                                                           MutableOperation.IGNORE_COMPLETION);
        return invokeOnPartition(cacheReplicationRemove.getKey(), operation);
    }

    private InternalCompletableFuture handleCacheUpdate(CacheReplicationUpdate cacheReplicationUpdate,
                                   CacheConfig cacheConfig) {
        EnterpriseCacheOperationProvider operationProvider;
        operationProvider = (EnterpriseCacheOperationProvider) cacheService
                                .getCacheOperationProvider(cacheReplicationUpdate.getNameWithPrefix(),
                                                           cacheConfig.getInMemoryFormat());
        CacheMergePolicy mergePolicy = cacheService
                .getCacheMergePolicyProvider().getMergePolicy(cacheReplicationUpdate.getMergePolicy());
        Operation operation =
                operationProvider.createWanMergeOperation(ORIGIN, cacheReplicationUpdate.getEntryView(),
                                                          mergePolicy, MutableOperation.IGNORE_COMPLETION);
        return invokeOnPartition(cacheReplicationUpdate.getKey(), operation);
    }

    private InternalCompletableFuture invokeOnPartition(Data key, Operation operation) {
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            return nodeEngine.getOperationService()
                    .invokeOnPartition(ICacheService.SERVICE_NAME, operation, partitionId);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }
}
