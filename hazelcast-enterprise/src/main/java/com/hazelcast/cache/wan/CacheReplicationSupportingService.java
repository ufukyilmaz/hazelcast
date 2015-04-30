package com.hazelcast.cache.wan;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.AbstractHazelcastCacheManager;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.cache.merge.CacheMergePolicy;
import com.hazelcast.cache.operation.EnterpriseCacheOperationProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.wan.WanReplicationEvent;

import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

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
            CachingProvider provider = Caching.getCachingProvider();
            Properties properties = HazelcastCachingProvider
                    .propertiesByInstanceName(nodeEngine.getConfig().getInstanceName());

            CacheConfig cacheConfig = null;
            try {
                cacheConfig = new CacheConfig(nodeEngine.getConfig().getCacheConfig(cacheReplicationObject.getCacheName()));
            } catch (Exception e) {
                ExceptionUtil.rethrow(e);
            }

            URI cacheManagerName = null;
            try {
                String uriString = cacheReplicationObject.getUriString();
                if (uriString != null) {
                    cacheManagerName = new URI(uriString);
                }
            } catch (URISyntaxException e) {
                ExceptionUtil.rethrow(e);
            }

            String cacheName = cacheReplicationObject.getCacheName();
            AbstractHazelcastCacheManager manager = (AbstractHazelcastCacheManager)
                    provider.getCacheManager(cacheManagerName, nodeEngine.getConfigClassLoader(), properties);
            synchronized (manager) {
                ICache cache = manager.getOrCreateCache(cacheName, cacheConfig);
                cacheConfig = (CacheConfig) cache.getConfiguration(CacheConfig.class);
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

    private void handleCacheRemove(WanReplicationEvent replicationEvent, CacheReplicationRemove cacheReplicationRemove,
                                   CacheConfig cacheConfig) {
        EnterpriseCacheOperationProvider operationProvider;
        operationProvider = (EnterpriseCacheOperationProvider) cacheService
                .getCacheOperationProvider(cacheConfig.getNameWithPrefix(), cacheConfig.getInMemoryFormat());
        Operation operation = operationProvider
                .createWanRemoveOperation(ORIGIN, cacheReplicationRemove.getKey(), null,
                        MutableOperation.IGNORE_COMPLETION);
        OperationService operationService = nodeEngine.getOperationService();
        int partitionId = getPartitionId(nodeEngine, cacheReplicationRemove.getKey());
        operationService.invokeOnPartition(replicationEvent.getServiceName(), operation, partitionId);
    }

    private void handleCacheUpdate(WanReplicationEvent replicationEvent, CacheReplicationUpdate cacheReplicationUpdate,
                                   CacheConfig cacheConfig) {
        EnterpriseCacheOperationProvider operationProvider;
        operationProvider = (EnterpriseCacheOperationProvider) cacheService
                .getCacheOperationProvider(cacheConfig.getNameWithPrefix(), cacheConfig.getInMemoryFormat());
        CacheMergePolicy mergePolicy = cacheService
                .getCacheMergePolicyProvider().getMergePolicy(cacheReplicationUpdate.getMergePolicy());
        Operation operation = operationProvider.createWanMergeOperation(
                ORIGIN,
                cacheReplicationUpdate.getEntryView().getKey(),
                cacheReplicationUpdate.getEntryView().getValue(), mergePolicy,
                cacheReplicationUpdate.getEntryView().getExpirationTime(), MutableOperation.IGNORE_COMPLETION);
        OperationService operationService = nodeEngine.getOperationService();
        int partitionId = getPartitionId(nodeEngine, cacheReplicationUpdate.getKey());
        operationService.invokeOnPartition(replicationEvent.getServiceName(), operation, partitionId);
    }
}
