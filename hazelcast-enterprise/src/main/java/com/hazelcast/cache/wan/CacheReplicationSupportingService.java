package com.hazelcast.cache.wan;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.EnterpriseCacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.PreJoinCacheConfig;
import com.hazelcast.cache.impl.operation.CacheCreateConfigOperation;
import com.hazelcast.cache.operation.EnterpriseCacheOperationProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationService;

import static com.hazelcast.cache.impl.operation.MutableOperation.IGNORE_COMPLETION;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;

/**
 * This class handles incoming cache WAN replication events.
 */
public class CacheReplicationSupportingService implements ReplicationSupportingService {

    /**
     * Event origin.
     */
    public static final String ORIGIN = "ENTERPRISE_WAN";

    private final EnterpriseCacheService cacheService;
    private final NodeEngine nodeEngine;
    private final ProxyService proxyService;
    private final WanReplicationService wanService;

    public CacheReplicationSupportingService(EnterpriseCacheService cacheService) {
        this.cacheService = cacheService;
        this.nodeEngine = cacheService.getNodeEngine();
        this.proxyService = nodeEngine.getProxyService();
        this.wanService = nodeEngine.getWanReplicationService();
    }

    @Override
    public void onReplicationEvent(WanReplicationEvent replicationEvent) {
        final Object eventObject = replicationEvent.getEventObject();
        if (!(eventObject instanceof CacheReplicationObject)) {
            return;
        }

        final CacheReplicationObject cacheReplicationObject = (CacheReplicationObject) eventObject;
        final CacheConfig cacheConfig = getCacheConfig(cacheReplicationObject);

        // Proxies should be created to initialize listeners, etc. and to show WAN replicated caches in mancenter.
        // Otherwise, users are forced to manually call cacheManager#getCache
        // Fixes https://github.com/hazelcast/hazelcast-enterprise/issues/1049
        proxyService.getDistributedObject(CacheService.SERVICE_NAME, cacheConfig.getNameWithPrefix());

        republishIfNecessary(replicationEvent, cacheConfig);

        if (cacheReplicationObject instanceof CacheReplicationUpdate) {
            handleUpdateEvent((CacheReplicationUpdate) cacheReplicationObject,
                    cacheConfig, replicationEvent.getAcknowledgeType());
            wanService.getReceivedEventCounters(ICacheService.SERVICE_NAME)
                      .incrementUpdate(cacheReplicationObject.getNameWithPrefix());
        } else if (cacheReplicationObject instanceof CacheReplicationRemove) {
            handleRemoveEvent((CacheReplicationRemove) cacheReplicationObject,
                    cacheConfig, replicationEvent.getAcknowledgeType());
            wanService.getReceivedEventCounters(ICacheService.SERVICE_NAME)
                      .incrementRemove(cacheReplicationObject.getNameWithPrefix());
        }
    }

    /**
     * Republishes the WAN {@code event} if configured to do so.
     *
     * @param event       the WAN replication event
     * @param cacheConfig the config for the cache on which this event
     *                    occurred
     */
    private void republishIfNecessary(WanReplicationEvent event, CacheConfig cacheConfig) {
        WanReplicationRef wanReplicationRef = cacheConfig.getWanReplicationRef();
        if (wanReplicationRef != null && wanReplicationRef.isRepublishingEnabled()) {
            cacheService.publishWanEvent(cacheConfig.getNameWithPrefix(), event);
        }
    }

    /**
     * Returns the existing local cache config or creates one if there is none.
     *
     * @param cacheReplicationObject the WAN replication object for the cache
     * @return the local cache config
     * @see CacheCreateConfigOperation
     */
    private CacheConfig getCacheConfig(CacheReplicationObject cacheReplicationObject) {
        CacheConfig cacheConfig;
        try {
            cacheConfig = getLocalCacheConfig(cacheReplicationObject.getNameWithPrefix(),
                    cacheReplicationObject.getCacheName());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        CacheConfig existingCacheConfig = cacheService.putCacheConfigIfAbsent(cacheConfig);
        if (existingCacheConfig == null) {
            cacheService.createCacheConfigOnAllMembers(PreJoinCacheConfig.of(cacheConfig));
        }
        return cacheConfig;
    }


    /**
     * Returns the local cache config or {@code null} if none was found.
     *
     * @param cacheNameWithPrefix the full name of the
     *                            {@link com.hazelcast.cache.ICache}, including
     *                            the manager scope prefix
     * @param cacheSimpleName     pure cache name without any prefix
     * @return the cache config or {@code null} if none was found
     */
    private CacheConfig getLocalCacheConfig(String cacheNameWithPrefix, String cacheSimpleName) {
        CacheConfig cacheConfig = cacheService.getCacheConfig(cacheNameWithPrefix);
        if (cacheConfig == null) {
            cacheConfig = cacheService.findCacheConfig(cacheSimpleName);
            if (cacheConfig != null) {
                cacheConfig.setManagerPrefix(cacheNameWithPrefix.substring(0, cacheNameWithPrefix.lastIndexOf(cacheSimpleName)));
            }
        }
        return cacheConfig;
    }

    /**
     * Processes a WAN remove event by forwarding it to the partition owner.
     * Depending on the {@code acknowledgeType}, it will either return as soon
     * as the event has been forwarded to the partition owner or block until
     * it has been processed on the partition owner.
     *
     * @param event           the WAN remove event
     * @param cacheConfig     the config for the cache on which this event
     *                        occurred
     * @param acknowledgeType determines whether the method will wait for the
     *                        update to be processed on the partition owner
     */
    private void handleRemoveEvent(CacheReplicationRemove event,
                                   CacheConfig cacheConfig,
                                   WanAcknowledgeType acknowledgeType) {
        final EnterpriseCacheOperationProvider operationProvider = (EnterpriseCacheOperationProvider) cacheService
                .getCacheOperationProvider(event.getNameWithPrefix(), cacheConfig.getInMemoryFormat());
        final Operation operation = operationProvider.createWanRemoveOperation(ORIGIN, event.getKey(),
                IGNORE_COMPLETION);
        final InternalCompletableFuture future = invokeOnPartition(event.getKey(), operation);
        if (future != null && acknowledgeType == WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE) {
            future.join();
        }
    }


    /**
     * Processes a WAN remove event by forwarding it to the partition owner.
     * Depending on the {@code acknowledgeType}, it will either return as soon
     * as the event has been forwarded to the partition owner or block until
     * it has been processed on the partition owner.
     *
     * @param event           the WAN remove event
     * @param cacheConfig     the config for the cache on which this event
     *                        occurred
     * @param acknowledgeType determines whether the method will wait for the
     *                        update to be processed on the partition owner
     */
    private void handleUpdateEvent(CacheReplicationUpdate event, CacheConfig cacheConfig, WanAcknowledgeType acknowledgeType) {
        EnterpriseCacheOperationProvider operationProvider = (EnterpriseCacheOperationProvider) cacheService
                .getCacheOperationProvider(event.getNameWithPrefix(), cacheConfig.getInMemoryFormat());
        Object mergePolicy = cacheService.getMergePolicyProvider().getMergePolicy(event.getMergePolicy());
        Operation operation;
        if (mergePolicy instanceof SplitBrainMergePolicy) {
            CacheMergeTypes mergingEntry = createMergingEntry(nodeEngine.getSerializationService(), event.getEntryView());
            //noinspection unchecked
            operation = operationProvider.createWanMergeOperation(ORIGIN, mergingEntry,
                    (SplitBrainMergePolicy<Data, CacheMergeTypes>) mergePolicy, IGNORE_COMPLETION);
        } else {
            CacheEntryView<Data, Data> entryView = event.getEntryView();
            operation = operationProvider.createLegacyWanMergeOperation(ORIGIN, entryView, (CacheMergePolicy) mergePolicy,
                    IGNORE_COMPLETION);
        }
        InternalCompletableFuture future = invokeOnPartition(event.getKey(), operation);
        if (future != null && acknowledgeType == WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE) {
            future.join();
        }
    }

    /**
     * Invokes the {@code operation} on the partition owner for the partition
     * owning the {@code key}.
     *
     * @param key       the key on which partition the operation is invoked
     * @param operation the operation to invoke
     * @return the future representing the pending completion of the operation
     */
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
