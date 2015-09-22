package com.hazelcast.cache.wan;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.impl.operation.CacheCreateConfigOperation;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.cache.operation.EnterpriseCacheOperationProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.wan.WanReplicationEvent;

import javax.cache.CacheException;

import static com.hazelcast.cache.impl.CacheProxyUtil.getPartitionId;

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
                cacheService.publishWanEvent(cacheConfig.getNameWithPrefix(), cacheReplicationObject);
            }

            if (cacheReplicationObject instanceof CacheReplicationUpdate) {
                handleCacheUpdate(replicationEvent, (CacheReplicationUpdate) cacheReplicationObject, cacheConfig);
            } else if (cacheReplicationObject instanceof CacheReplicationRemove) {
                handleCacheRemove(replicationEvent, (CacheReplicationRemove) cacheReplicationObject, cacheConfig);
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

    private void handleCacheRemove(WanReplicationEvent replicationEvent, CacheReplicationRemove cacheReplicationRemove,
                                   CacheConfig cacheConfig) {
        EnterpriseCacheOperationProvider operationProvider;
        operationProvider = (EnterpriseCacheOperationProvider) cacheService
                                .getCacheOperationProvider(cacheReplicationRemove.getNameWithPrefix(),
                                                           cacheConfig.getInMemoryFormat());
        Operation operation =
                operationProvider.createWanRemoveOperation(ORIGIN, cacheReplicationRemove.getKey(),
                                                           MutableOperation.IGNORE_COMPLETION);
        OperationService operationService = nodeEngine.getOperationService();
        int partitionId = getPartitionId(nodeEngine, cacheReplicationRemove.getKey());
        operationService.invokeOnPartition(replicationEvent.getServiceName(), operation, partitionId);
    }

    private void handleCacheUpdate(WanReplicationEvent replicationEvent, CacheReplicationUpdate cacheReplicationUpdate,
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
        OperationService operationService = nodeEngine.getOperationService();
        int partitionId = getPartitionId(nodeEngine, cacheReplicationUpdate.getKey());
        operationService.invokeOnPartition(replicationEvent.getServiceName(), operation, partitionId);
    }
}
