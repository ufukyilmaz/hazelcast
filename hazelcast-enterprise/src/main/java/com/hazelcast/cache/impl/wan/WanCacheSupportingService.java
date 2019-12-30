package com.hazelcast.cache.impl.wan;

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.EnterpriseCacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.PreJoinCacheConfig;
import com.hazelcast.cache.impl.operation.EnterpriseCacheOperationProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.WanSupportingService;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.proxyservice.ProxyService;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;
import com.hazelcast.wan.impl.InternalWanEvent;
import com.hazelcast.wan.impl.WanReplicationService;

import java.util.UUID;

import static com.hazelcast.cache.impl.operation.MutableOperation.IGNORE_COMPLETION;
import static com.hazelcast.internal.util.UuidUtil.NIL_UUID;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;

/**
 * This class handles incoming cache WAN replication events.
 */
public class WanCacheSupportingService implements WanSupportingService {

    /**
     * Event origin.
     */
    public static final UUID ORIGIN = NIL_UUID;

    private final EnterpriseCacheService cacheService;
    private final NodeEngine nodeEngine;
    private final ProxyService proxyService;
    private final WanReplicationService wanService;

    public WanCacheSupportingService(EnterpriseCacheService cacheService) {
        this.cacheService = cacheService;
        this.nodeEngine = cacheService.getNodeEngine();
        this.proxyService = nodeEngine.getProxyService();
        this.wanService = nodeEngine.getWanReplicationService();
    }

    @Override
    public void onReplicationEvent(InternalWanEvent event, WanAcknowledgeType acknowledgeType) {
        if (!(event instanceof WanEnterpriseCacheEvent)) {
            return;
        }

        final WanEnterpriseCacheEvent wanCacheEvent = (WanEnterpriseCacheEvent) event;
        final CacheConfig cacheConfig = getCacheConfig(wanCacheEvent);

        // Proxies should be created to initialize listeners, etc. and to show WAN replicated caches in MC.
        // Otherwise, users are forced to manually call cacheManager#getCache
        // Fixes https://github.com/hazelcast/hazelcast-enterprise/issues/1049
        UUID source = nodeEngine.getLocalMember().getUuid();
        proxyService.getDistributedObject(CacheService.SERVICE_NAME, cacheConfig.getNameWithPrefix(), source);

        republishIfNecessary(event, cacheConfig);

        if (wanCacheEvent instanceof WanEnterpriseCacheAddOrUpdateEvent) {
            handleAddOrUpdateEvent((WanEnterpriseCacheAddOrUpdateEvent) wanCacheEvent, cacheConfig, acknowledgeType);
            wanService.getReceivedEventCounters(ICacheService.SERVICE_NAME)
                    .incrementUpdate(wanCacheEvent.getNameWithPrefix());
        } else if (wanCacheEvent instanceof WanEnterpriseCacheRemoveEvent) {
            handleRemoveEvent((WanEnterpriseCacheRemoveEvent) wanCacheEvent, cacheConfig, acknowledgeType);
            wanService.getReceivedEventCounters(ICacheService.SERVICE_NAME)
                    .incrementRemove(wanCacheEvent.getNameWithPrefix());
        }
    }

    /**
     * Republishes the WAN {@code event} if configured to do so.
     *
     * @param event       the WAN replication event
     * @param cacheConfig the config for the cache on which this event
     *                    occurred
     */
    private void republishIfNecessary(InternalWanEvent event, CacheConfig cacheConfig) {
        WanReplicationRef wanReplicationRef = cacheConfig.getWanReplicationRef();
        if (wanReplicationRef != null && wanReplicationRef.isRepublishingEnabled()) {
            cacheService.publishWanEvent(cacheConfig.getNameWithPrefix(), event);
        }
    }

    /**
     * Returns the existing local cache config or creates one if there is none.
     *
     * @param wanCacheEvent the WAN replication object for the cache
     * @return the local cache config
     * @see com.hazelcast.cache.impl.operation.AddCacheConfigOperation
     */
    private CacheConfig getCacheConfig(WanEnterpriseCacheEvent wanCacheEvent) {
        CacheConfig cacheConfig;
        try {
            cacheConfig = getLocalCacheConfig(wanCacheEvent.getNameWithPrefix(),
                    wanCacheEvent.getCacheName());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        // add the cache config if it does not exist yet
        CacheConfig existingCacheConfig = cacheService.putCacheConfigIfAbsent(cacheConfig);
        if (existingCacheConfig == null) {
            cacheService.createCacheConfigOnAllMembers(PreJoinCacheConfig.of(cacheConfig));
        }
        return cacheConfig;
    }

    /**
     * Returns the local cache config corresponding to the given cache name.
     * If a direct lookup on the prefixed cache name yields no result, performs
     * a pattern match using the simple cache name.
     *
     * @param cacheNameWithPrefix the full name of the
     *                            {@link com.hazelcast.cache.ICache}, including
     *                            the manager scope prefix
     * @param cacheSimpleName     pure cache name without any prefix
     * @return the cache config
     * @throws CacheNotExistsException if a matching local cache config cannot be found
     */
    private CacheConfig getLocalCacheConfig(String cacheNameWithPrefix, String cacheSimpleName) {
        CacheConfig cacheConfig = cacheService.getCacheConfig(cacheNameWithPrefix);
        if (cacheConfig == null) {
            cacheConfig = cacheService.findCacheConfig(cacheSimpleName);
            if (cacheConfig == null) {
                throw new CacheNotExistsException("Couldn't find cache config with name " + cacheNameWithPrefix);
            } else {
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
    private void handleRemoveEvent(WanEnterpriseCacheRemoveEvent event,
                                   CacheConfig cacheConfig,
                                   WanAcknowledgeType acknowledgeType) {
        final EnterpriseCacheOperationProvider operationProvider = (EnterpriseCacheOperationProvider) cacheService
                .getCacheOperationProvider(event.getNameWithPrefix(), cacheConfig.getInMemoryFormat());
        final Operation operation = operationProvider.createWanRemoveOperation(ORIGIN, event.getKey(),
                IGNORE_COMPLETION);
        final InternalCompletableFuture future = invokeOnPartition(event.getKey(), operation);
        if (future != null && acknowledgeType == WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE) {
            future.joinInternal();
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
    private void handleAddOrUpdateEvent(WanEnterpriseCacheAddOrUpdateEvent event,
                                        CacheConfig cacheConfig, WanAcknowledgeType acknowledgeType) {

        EnterpriseCacheOperationProvider operationProvider = (EnterpriseCacheOperationProvider) cacheService
                .getCacheOperationProvider(event.getNameWithPrefix(), cacheConfig.getInMemoryFormat());

        SplitBrainMergePolicy mergePolicy = cacheService.getMergePolicyProvider().getMergePolicy(event.getMergePolicy());
        CacheMergeTypes mergingEntry = createMergingEntry(nodeEngine.getSerializationService(), event.getEntryView());
        Operation operation = operationProvider.createWanMergeOperation(mergingEntry,
                (SplitBrainMergePolicy<Data, CacheMergeTypes>) mergePolicy, IGNORE_COMPLETION);

        InternalCompletableFuture future = invokeOnPartition(event.getKey(), operation);

        if (future != null && acknowledgeType == WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE) {
            future.joinInternal();
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
