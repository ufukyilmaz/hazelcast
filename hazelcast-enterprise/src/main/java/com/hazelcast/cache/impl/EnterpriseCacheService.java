package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheEventType;
import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.hidensity.HiDensityCacheStorageInfo;
import com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecordStore;
import com.hazelcast.cache.hidensity.impl.nativememory.HotRestartHiDensityNativeMemoryCacheRecordStore;
import com.hazelcast.cache.hidensity.operation.CacheSegmentShutdownOperation;
import com.hazelcast.cache.hidensity.operation.HiDensityCacheOperationProvider;
import com.hazelcast.cache.hidensity.operation.HiDensityCacheReplicationOperation;
import com.hazelcast.cache.hotrestart.HotRestartEnterpriseCacheRecordStore;
import com.hazelcast.cache.impl.event.CacheWanEventPublisher;
import com.hazelcast.cache.impl.event.CacheWanEventPublisherImpl;
import com.hazelcast.cache.impl.merge.entry.LazyCacheEntryView;
import com.hazelcast.cache.impl.operation.CacheReplicationOperation;
import com.hazelcast.cache.impl.wan.CacheFilterProvider;
import com.hazelcast.cache.operation.CacheSegmentDestroyOperation;
import com.hazelcast.cache.operation.EnterpriseCacheOperationProvider;
import com.hazelcast.cache.operation.WANAwareCacheOperationProvider;
import com.hazelcast.cache.wan.CacheReplicationRemove;
import com.hazelcast.cache.wan.CacheReplicationSupportingService;
import com.hazelcast.cache.wan.CacheReplicationUpdate;
import com.hazelcast.cache.wan.WanCacheEntryView;
import com.hazelcast.cache.wan.filter.CacheWanEventFilter;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.enterprise.wan.WanFilterEventType;
import com.hazelcast.instance.impl.EnterpriseNodeExtension;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.util.InvocationUtil;
import com.hazelcast.internal.util.LocalRetryableExecution;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.spi.hotrestart.HotRestartIntegrationService;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.LoadedConfigurationListener;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.RamStoreRegistry;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.CollectionUtil;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;
import com.hazelcast.wan.WanReplicationService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hazelcast.cache.impl.AbstractCacheRecordStore.SOURCE_NOT_AVAILABLE;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.spi.hotrestart.PersistentConfigDescriptors.toPartitionId;
import static java.lang.Thread.currentThread;

/**
 * The {@link ICacheService} implementation specified for enterprise usage.
 * <p>
 * This {@link EnterpriseCacheService} implementation mainly handles
 * <ul>
 * <li>{@link ICacheRecordStore} creation of caches with specified partition ID</li>
 * <li>Destroying segments and caches</li>
 * <li>Mediating for cache events and listeners</li>
 * </ul>
 * <p>
 * When interacting with Hot Restart persistent stores, cache configurations must be persisted in the serialized form of
 * {@link PreJoinCacheConfig}.
 */
@SuppressWarnings({
        "checkstyle:methodcount",
        "checkstyle:classdataabstractioncoupling",
        "checkstyle:classfanoutcomplexity"
})
public class EnterpriseCacheService
        extends CacheService
        implements ReplicationSupportingService, RamStoreRegistry {

    private static final int CACHE_SEGMENT_DESTROY_OPERATION_AWAIT_TIME_IN_SECS = 30;

    private final ConcurrentMap<String, WanReplicationPublisher> wanReplicationPublishers =
            new ConcurrentHashMap<String, WanReplicationPublisher>();
    private final ConcurrentMap<String, String> cacheMergePolicies =
            new ConcurrentHashMap<String, String>();
    private final ConcurrentMap<String, HiDensityStorageInfo> hiDensityCacheInfoMap =
            new ConcurrentHashMap<String, HiDensityStorageInfo>();
    private final ConstructorFunction<String, HiDensityStorageInfo> hiDensityCacheInfoConstructorFunction =
            new ConstructorFunction<String, HiDensityStorageInfo>() {
                @Override
                public HiDensityStorageInfo createNew(String cacheNameWithPrefix) {
                    CacheConfig cacheConfig = getCacheConfig(cacheNameWithPrefix);
                    if (cacheConfig == null) {
                        throw new CacheNotExistsException("Cache " + cacheNameWithPrefix
                                + " is already destroyed or not created yet, on " + nodeEngine.getLocalMember());
                    }
                    CacheContext cacheContext = getOrCreateCacheContext(cacheNameWithPrefix);
                    HiDensityCacheStorageInfo storageInfo = new HiDensityCacheStorageInfo(cacheNameWithPrefix, cacheContext);
                    registerCacheProbes(storageInfo, cacheNameWithPrefix);
                    return storageInfo;
                }
            };

    private IPartitionService partitionService;
    private CacheFilterProvider cacheFilterProvider;
    private CacheWanEventPublisher cacheWanEventPublisher;
    private HotRestartIntegrationService hotRestartService;
    private ReplicationSupportingService replicationSupportingService;

    @Override
    protected void postInit(NodeEngine nodeEngine, Properties properties) {
        super.postInit(nodeEngine, properties);
        replicationSupportingService = new CacheReplicationSupportingService(this);
        cacheFilterProvider = new CacheFilterProvider(nodeEngine);
        cacheWanEventPublisher = new CacheWanEventPublisherImpl(this);
        partitionService = nodeEngine.getPartitionService();

        hotRestartService = getHotRestartService();
        if (hotRestartService != null) {
            hotRestartService.registerRamStoreRegistry(SERVICE_NAME, this);
            hotRestartService.registerLoadedConfigurationListener(new LoadedConfigurationListener() {
                @Override
                public void onConfigurationLoaded(String serviceName, String name, Object config) {
                    if (SERVICE_NAME.equals(serviceName)) {
                        if (config instanceof CacheConfig) {
                            putCacheConfigIfAbsent((CacheConfig) config);
                        } else {
                            logger.warning("Configuration " + config + " has an unknown type " + config.getClass());
                        }
                    }
                }
            });
        }
    }

    @Override
    public RamStore ramStoreForPrefix(long prefix) {
        String name = hotRestartService.getCacheName(prefix);
        return (RamStore) getRecordStore(name, toPartitionId(prefix));
    }

    @Override
    public RamStore restartingRamStoreForPrefix(long prefix) {
        String name = hotRestartService.getCacheName(prefix);
        return (RamStore) getOrCreateRecordStore(name, toPartitionId(prefix));
    }

    @Override
    public int prefixToThreadId(long prefix) {
        throw new UnsupportedOperationException();
    }

    public HotRestartStore onHeapHotRestartStoreForPartition(int partitionId) {
        return hotRestartService.getOnHeapHotRestartStoreForPartition(partitionId);
    }

    public HotRestartStore offHeapHotRestartStoreForPartition(int partitionId) {
        return hotRestartService.getOffHeapHotRestartStoreForPartition(partitionId);
    }

    /**
     * Creates new {@link ICacheRecordStore} as specified {@link InMemoryFormat}.
     *
     * @param cacheNameWithPrefix the full name of the cache, including the manager scope prefix
     * @param partitionId         the partition ID which cache record store is created on
     * @return the created {@link ICacheRecordStore}
     * @see com.hazelcast.cache.impl.CacheRecordStore
     * @see com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecordStore
     */
    @Override
    protected ICacheRecordStore createNewRecordStore(String cacheNameWithPrefix, int partitionId) {
        CacheConfig cacheConfig = getCacheConfig(cacheNameWithPrefix);
        if (cacheConfig == null) {
            throw new CacheNotExistsException("Cache is already destroyed or not created yet, on "
                    + nodeEngine.getLocalMember());
        }
        InMemoryFormat inMemoryFormat = cacheConfig.getInMemoryFormat();
        boolean isNative;
        switch (inMemoryFormat) {
            case NATIVE:
                isNative = true;
                break;
            case BINARY:
            case OBJECT:
                isNative = false;
                break;
            default:
                throw new IllegalArgumentException(
                        "Cannot create record store for the storage type: " + inMemoryFormat);
        }

        long prefix = 0L;
        HotRestartConfig hotRestartConfig = getHotRestartConfig(cacheConfig);
        if (hotRestartConfig.isEnabled()) {
            if (hotRestartService == null) {
                throw new HazelcastException("Hot Restart is enabled for cache: " + cacheConfig.getName()
                        + " but Hot Restart persistence is not enabled!");
            }

            // store in hot restart persistent store with PreJoinCacheConfig format
            hotRestartService.ensureHasConfiguration(SERVICE_NAME, cacheNameWithPrefix,
                    new PreJoinCacheConfig(cacheConfig, false));
            prefix = hotRestartService.registerRamStore(this, SERVICE_NAME, cacheNameWithPrefix, partitionId);
            nodeEngine.getProxyService().initializeDistributedObject(SERVICE_NAME, cacheNameWithPrefix);
        }
        return isNative
                ? newNativeRecordStore(cacheNameWithPrefix, partitionId, hotRestartConfig, prefix)
                : newHeapRecordStore(cacheNameWithPrefix, partitionId, hotRestartConfig, prefix);
    }

    private static HotRestartConfig getHotRestartConfig(CacheConfig cacheConfig) {
        HotRestartConfig hotRestartConfig = cacheConfig.getHotRestartConfig();
        return hotRestartConfig != null ? hotRestartConfig : new HotRestartConfig().setEnabled(false);
    }

    private HotRestartIntegrationService getHotRestartService() {
        NodeEngineImpl nodeEngineImpl = (NodeEngineImpl) nodeEngine;
        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) nodeEngineImpl.getNode().getNodeExtension();
        return nodeExtension.isHotRestartEnabled()
                ? (HotRestartIntegrationService) nodeExtension.getInternalHotRestartService() : null;
    }

    private ICacheRecordStore newHeapRecordStore(String name, int partitionId, HotRestartConfig hotRestart, long prefix) {
        return hotRestart.isEnabled()
                ? new HotRestartEnterpriseCacheRecordStore(name, partitionId, nodeEngine, this, hotRestart.isFsync(), prefix)
                : new CacheRecordStore(name, partitionId, nodeEngine, this);
    }

    private ICacheRecordStore newNativeRecordStore(String cacheNameWithPrefix, int partitionId,
                                                   HotRestartConfig hotRestart, long prefix) {
        try {
            return hotRestart.isEnabled()
                    ? new HotRestartHiDensityNativeMemoryCacheRecordStore(partitionId, cacheNameWithPrefix, this, nodeEngine,
                    hotRestart.isFsync(), prefix)
                    : new HiDensityNativeMemoryCacheRecordStore(partitionId, cacheNameWithPrefix, this, nodeEngine);
        } catch (NativeOutOfMemoryError e) {
            throw new NativeOutOfMemoryError("Cannot create internal cache map, "
                    + "not enough contiguous memory available! -> " + e.getMessage(), e);
        }
    }

    /**
     * Destroys the segments for specified cache.
     *
     * @param cacheConfig the configuration of the cache whose segments will be destroyed
     */
    @Override
    protected void destroySegments(CacheConfig cacheConfig) {
        if (cacheConfig.getInMemoryFormat() != NATIVE) {
            super.destroySegments(cacheConfig);
            return;
        }

        String cacheNameWithPrefix = cacheConfig.getNameWithPrefix();
        destroySegmentsInternal(cacheNameWithPrefix);
        HiDensityStorageInfo storageInfo = hiDensityCacheInfoMap.remove(cacheNameWithPrefix);
        if (storageInfo != null) {
            deregisterCacheProbes(storageInfo);
        }
    }

    /**
     * Destroys the cache segments on local partition threads and waits for
     * {@value #CACHE_SEGMENT_DESTROY_OPERATION_AWAIT_TIME_IN_SECS} seconds
     * for each cache segment destruction to complete.
     *
     * @param cacheNameWithPrefix the name of the cache (including manager prefix)
     */
    private void destroySegmentsInternal(String cacheNameWithPrefix) {
        OperationService operationService = nodeEngine.getOperationService();
        List<LocalRetryableExecution> executions = new ArrayList<LocalRetryableExecution>();
        for (CachePartitionSegment segment : segments) {
            if (segment.hasRecordStore(cacheNameWithPrefix)) {
                CacheSegmentDestroyOperation op = new CacheSegmentDestroyOperation(cacheNameWithPrefix);
                op.setPartitionId(segment.getPartitionId());
                op.setNodeEngine(nodeEngine).setService(this);
                if (operationService.isRunAllowed(op)) {
                    operationService.run(op);
                } else {
                    executions.add(InvocationUtil.executeLocallyWithRetry(nodeEngine, op));
                }
            }
        }
        for (LocalRetryableExecution execution : executions) {
            try {
                if (!execution.awaitCompletion(
                        CACHE_SEGMENT_DESTROY_OPERATION_AWAIT_TIME_IN_SECS, TimeUnit.SECONDS)) {
                    logger.warning("Cache segment was not destroyed in expected time, possible leak");
                }
            } catch (InterruptedException e) {
                currentThread().interrupt();
                nodeEngine.getLogger(getClass()).warning(e);
            }
        }
    }

    private void registerCacheProbes(HiDensityStorageInfo cacheInfo, String cacheName) {
        ((NodeEngineImpl) nodeEngine).getMetricsRegistry().scanAndRegister(cacheInfo, "cache[" + cacheName + "]");
    }

    private void deregisterCacheProbes(HiDensityStorageInfo cacheInfo) {
        ((NodeEngineImpl) nodeEngine).getMetricsRegistry().deregister(cacheInfo);
    }

    /**
     * Shutdowns the cache service and destroy the caches with their segments.
     *
     * @param terminate condition about cache service will be closed or not
     */
    @Override
    public void shutdown(boolean terminate) {
        OperationService operationService = nodeEngine.getOperationService();
        List<CacheSegmentShutdownOperation> ops = new ArrayList<CacheSegmentShutdownOperation>();
        for (CachePartitionSegment segment : segments) {
            if (segment.hasAnyRecordStore()) {
                CacheSegmentShutdownOperation op = new CacheSegmentShutdownOperation();
                op.setPartitionId(segment.getPartitionId());
                op.setNodeEngine(nodeEngine).setService(this);
                if (operationService.isRunAllowed(op)) {
                    operationService.run(op);
                } else {
                    operationService.execute(op);
                    ops.add(op);
                }
            }
        }
        for (CacheSegmentShutdownOperation op : ops) {
            try {
                op.awaitCompletion(CACHE_SEGMENT_DESTROY_OPERATION_AWAIT_TIME_IN_SECS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                currentThread().interrupt();
                nodeEngine.getLogger(getClass()).warning(e);
            }
        }
        hiDensityCacheInfoMap.clear();
    }

    /**
     * Resets the cache service without closing.
     */
    @Override
    public void reset() {
        shutdown(false);
    }

    /**
     * Does forced eviction on one or more caches. Runs on the operation threads.
     *
     * @param name                the name of the cache to be evicted
     * @param originalPartitionId the partition ID of the record store stores the records of cache
     * @return the number of evicted records
     */
    public int forceEvict(String name, int originalPartitionId) {
        if (logger.isFinestEnabled()) {
            logger.finest("Forced eviction " + name + ", original partition ID: " + originalPartitionId);
        }
        int evicted = 0;
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        int threadCount = getPartitionThreadCount();
        int mod = originalPartitionId % threadCount;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            if (partitionId % threadCount == mod) {
                ICacheRecordStore cache = getRecordStore(name, partitionId);
                if (cache instanceof HiDensityCacheRecordStore) {
                    evicted += ((HiDensityCacheRecordStore) cache).forceEvict();
                }
            }
        }
        return evicted;
    }

    private int getPartitionThreadCount() {
        return nodeEngine.getOperationService().getPartitionThreadCount();
    }

    /**
     * Does forced eviction on other caches. Runs on the operation threads.
     *
     * @param name                the name of the cache not to be evicted.
     * @param originalPartitionId the partition ID of the record store that stores the records of cache
     * @return the number of evicted records
     */
    public int forceEvictOnOthers(String name, int originalPartitionId) {
        int evicted = 0;
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        int threadCount = getPartitionThreadCount();
        int mod = originalPartitionId % threadCount;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            if (partitionId % threadCount == mod) {
                for (CacheConfig cacheConfig : getCacheConfigs()) {
                    String cacheName = cacheConfig.getNameWithPrefix();
                    if (!cacheName.equals(name)) {
                        ICacheRecordStore cache = getRecordStore(cacheName, partitionId);
                        if (cache instanceof HiDensityCacheRecordStore) {
                            evicted += ((HiDensityCacheRecordStore) cache).forceEvict();
                        }
                    }
                }
            }
        }
        return evicted;
    }

    /**
     * Clears all record stores on the partitions owned by partition thread of original partition.
     *
     * @param originalPartitionId the ID of original partition
     */
    public void clearAll(int originalPartitionId) {
        NodeEngine nodeEngine = getNodeEngine();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        int threadCount = getPartitionThreadCount();
        int mod = originalPartitionId % threadCount;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            if (partitionId % threadCount == mod) {
                for (CacheConfig cacheConfig : getCacheConfigs()) {
                    String cacheName = cacheConfig.getNameWithPrefix();
                    ICacheRecordStore cache = getRecordStore(cacheName, partitionId);
                    if (cache != null) {
                        cache.clear();
                        sendInvalidationEvent(cacheName, null, SOURCE_NOT_AVAILABLE);
                    }
                }
            }
        }
    }

    /**
     * Creates a {@link HiDensityCacheReplicationOperation} to start the replication.
     */
    @Override
    protected CacheReplicationOperation newCacheReplicationOperation() {
        return new HiDensityCacheReplicationOperation();
    }

    /**
     * Creates a {@link CacheOperationProvider} as specified {@link InMemoryFormat}
     * for specified {@code cacheNameWithPrefix}.
     *
     * @param cacheNameWithPrefix the name of the cache (including manager prefix) that operation works on
     * @param inMemoryFormat      the format of memory such as {@code BINARY}, {@code OBJECT}
     *                            or {@code NATIVE}
     */
    @Override
    protected CacheOperationProvider createOperationProvider(String cacheNameWithPrefix, InMemoryFormat inMemoryFormat) {
        EnterpriseCacheOperationProvider operationProvider;
        if (NATIVE.equals(inMemoryFormat)) {
            operationProvider = new HiDensityCacheOperationProvider(cacheNameWithPrefix);
        } else {
            operationProvider = new EnterpriseCacheOperationProvider(cacheNameWithPrefix);
        }

        if (isWanReplicationEnabled(cacheNameWithPrefix)) {
            return new WANAwareCacheOperationProvider(cacheNameWithPrefix,
                    operationProvider, new WanPublisherLookup());
        } else {
            return operationProvider;
        }
    }

    @Override
    public CacheOperationProvider getCacheOperationProvider(String cacheNameWithPrefix, InMemoryFormat inMemoryFormat) {
        CacheOperationProvider cacheOperationProvider = operationProviderCache.get(cacheNameWithPrefix);
        if (cacheOperationProvider != null) {
            return cacheOperationProvider;
        }
        cacheOperationProvider = createOperationProvider(cacheNameWithPrefix, inMemoryFormat);
        CacheOperationProvider current = operationProviderCache.putIfAbsent(cacheNameWithPrefix, cacheOperationProvider);
        return current == null ? cacheOperationProvider : current;
    }

    /**
     * Gets the {@link EnterpriseSerializationService} used by this {@link ICacheService}.
     *
     * @return the used {@link EnterpriseSerializationService}
     */
    public EnterpriseSerializationService getSerializationService() {
        return (EnterpriseSerializationService) nodeEngine.getSerializationService();
    }

    /**
     * Gets or creates (if there is no cache info for that Hi-Density cache) {@link HiDensityStorageInfo} instance
     * which holds live information about cache.
     *
     * @param cacheNameWithPrefix Name (including prefix) of the cache whose live information is requested
     * @return the {@link HiDensityStorageInfo} instance which holds live information about Hi-Density cache
     */
    public HiDensityStorageInfo getOrCreateHiDensityCacheInfo(String cacheNameWithPrefix) {
        return ConcurrencyUtil.getOrPutSynchronized(hiDensityCacheInfoMap, cacheNameWithPrefix,
                this, hiDensityCacheInfoConstructorFunction);
    }

    @Override
    protected void additionalCacheConfigSetup(CacheConfig config, boolean existingConfig) {
        if (!existingConfig) {
            if (hotRestartService != null && config.getHotRestartConfig().isEnabled()) {
                hotRestartService.ensureHasConfiguration(SERVICE_NAME, config.getNameWithPrefix(),
                        new PreJoinCacheConfig(config, false));
            }
        }

        WanReplicationRef wanReplicationRef = config.getWanReplicationRef();
        if (wanReplicationRef != null) {
            WanReplicationService wanReplicationService = nodeEngine.getWanReplicationService();
            WanReplicationPublisher publisher = wanReplicationService.getWanReplicationPublisher(
                    wanReplicationRef.getName());
            if (publisher == null) {
                String msg = String.format("Missing WAN replication config with name '%s' for cache '%s'",
                        wanReplicationRef.getName(), config.getNameWithPrefix());
                throw new InvalidConfigurationException(msg);
            }
            wanReplicationPublishers.putIfAbsent(config.getNameWithPrefix(), publisher);
            cacheMergePolicies.putIfAbsent(config.getNameWithPrefix(), wanReplicationRef.getMergePolicy());
        }
    }

    @Override
    public CacheConfig deleteCacheConfig(String cacheNameWithPrefix) {
        wanReplicationPublishers.remove(cacheNameWithPrefix);
        return super.deleteCacheConfig(cacheNameWithPrefix);
    }

    @Override
    public void onReplicationEvent(WanReplicationEvent wanReplicationEvent) {
        replicationSupportingService.onReplicationEvent(wanReplicationEvent);
    }

    public void publishWanEvent(CacheEventContext cacheEventContext) {
        String cacheName = cacheEventContext.getCacheName();
        CacheEventType eventType = cacheEventContext.getEventType();
        WanReplicationPublisher wanReplicationPublisher = getOrLookupWanPublisher(cacheEventContext.getCacheName());
        if (wanReplicationPublisher != null && cacheEventContext.getOrigin() == null) {
            CacheConfig config = getCacheConfig(cacheName);
            WanReplicationRef wanReplicationRef = config.getWanReplicationRef();
            List<String> filters = getFiltersFrom(wanReplicationRef);

            if (isEventFiltered(cacheEventContext, filters)) {
                return;
            }

            boolean backup = !isOwnedPartition(cacheEventContext.getDataKey());

            if (eventType == CacheEventType.UPDATED
                    || eventType == CacheEventType.CREATED
                    || eventType == CacheEventType.EXPIRATION_TIME_UPDATED) {
                CacheReplicationUpdate update =
                        new CacheReplicationUpdate(
                                config.getName(),
                                cacheMergePolicies.get(cacheName),
                                new WanCacheEntryView(
                                        cacheEventContext.getDataKey(),
                                        cacheEventContext.getDataValue(),
                                        cacheEventContext.getCreationTime(),
                                        cacheEventContext.getExpirationTime(),
                                        cacheEventContext.getLastAccessTime(),
                                        cacheEventContext.getAccessHit()),
                                config.getManagerPrefix(), config.getTotalBackupCount());
                if (backup) {
                    wanReplicationPublisher.publishReplicationEventBackup(SERVICE_NAME, update);
                } else {
                    wanReplicationPublisher.publishReplicationEvent(SERVICE_NAME, update);
                }
            } else if (eventType == CacheEventType.REMOVED) {
                CacheReplicationRemove remove = new CacheReplicationRemove(config.getName(), cacheEventContext.getDataKey(),
                        config.getManagerPrefix(), config.getTotalBackupCount());
                if (backup) {
                    wanReplicationPublisher.publishReplicationEventBackup(SERVICE_NAME, remove);
                } else {
                    wanReplicationPublisher.publishReplicationEvent(SERVICE_NAME, remove);
                }
            }
        }
    }

    private static List<String> getFiltersFrom(WanReplicationRef wanReplicationRef) {
        if (wanReplicationRef == null) {
            return Collections.emptyList();
        }

        List<String> filters = wanReplicationRef.getFilters();
        return CollectionUtil.isEmpty(filters) ? Collections.<String>emptyList() : filters;
    }

    private boolean isOwnedPartition(Data dataKey) {
        int partitionId = partitionService.getPartitionId(dataKey);
        return partitionService.getPartition(partitionId, false).isLocal();
    }

    private boolean isEventFiltered(CacheEventContext eventContext, List<String> filters) {
        if (!filters.isEmpty()) {
            CacheEntryView entryView =
                    new LazyCacheEntryView(
                            eventContext.getDataKey(),
                            eventContext.getDataValue(),
                            eventContext.getCreationTime(),
                            eventContext.getExpirationTime(),
                            eventContext.getLastAccessTime(),
                            eventContext.getAccessHit(),
                            eventContext.getExpiryPolicy(),
                            getSerializationService());
            WanFilterEventType eventType = convertWanFilterEventType(eventContext.getEventType());
            for (String filterName : filters) {
                CacheWanEventFilter filter = cacheFilterProvider.getFilter(filterName);
                if (filter.filter(eventContext.getCacheName(), entryView, eventType)) {
                    return true;
                }
            }
        }
        return false;
    }

    private WanFilterEventType convertWanFilterEventType(CacheEventType eventType) {
        if (eventType == CacheEventType.REMOVED) {
            return WanFilterEventType.REMOVED;
        }
        return WanFilterEventType.UPDATED;
    }

    /**
     * Publishes a WAN event for the provided {@code cacheNameWithPrefix} if there
     * is a publisher initialized for the cache. It is possible that the publisher has
     * not been initialised yet for this cache which means that the event may not be published
     * at this point but may be published at a later point.
     *
     * @param cacheNameWithPrefix the full name of the cache, including the manager scope prefix
     * @param wanReplicationEvent the WAN event to be published
     */
    public void publishWanEvent(String cacheNameWithPrefix, WanReplicationEvent wanReplicationEvent) {
        final WanReplicationPublisher wanReplicationPublisher = getOrLookupWanPublisher(cacheNameWithPrefix);
        if (wanReplicationPublisher != null) {
            wanReplicationPublisher.publishReplicationEvent(wanReplicationEvent);
        }
    }

    private WanReplicationPublisher getOrLookupWanPublisher(String cacheNameWithPrefix) {
        WanReplicationPublisher publisher = wanReplicationPublishers.get(cacheNameWithPrefix);
        if (publisher == null) {
            CacheConfig cacheConfig = getCacheConfig(cacheNameWithPrefix);
            WanReplicationRef wanReplicationRef = cacheConfig.getWanReplicationRef();
            publisher = nodeEngine.getWanReplicationService().getWanReplicationPublisher(wanReplicationRef.getName());
            WanReplicationPublisher replacedPublisher = wanReplicationPublishers.putIfAbsent(cacheNameWithPrefix, publisher);
            if (replacedPublisher != null) {
                return replacedPublisher;
            }
        }

        return publisher;
    }

    @Override
    public boolean isWanReplicationEnabled(String cacheNameWithPrefix) {
        final CacheConfig config = getCacheConfig(cacheNameWithPrefix);
        final WanReplicationRef wanReplicationRef = config.getWanReplicationRef();
        final WanReplicationService wanService = nodeEngine.getWanReplicationService();

        return wanReplicationRef != null
                && wanService.getWanReplicationPublisher(wanReplicationRef.getName()) != null;
    }

    @Override
    public CacheWanEventPublisher getCacheWanEventPublisher() {
        return cacheWanEventPublisher;
    }

    private class WanPublisherLookup implements Function<String, WanReplicationPublisher> {

        @Override
        public WanReplicationPublisher apply(String cacheNameWithPrefix) {
            return getOrLookupWanPublisher(cacheNameWithPrefix);
        }
    }
}
