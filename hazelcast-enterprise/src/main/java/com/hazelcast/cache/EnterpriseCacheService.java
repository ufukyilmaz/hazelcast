package com.hazelcast.cache;

import com.hazelcast.cache.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.hidensity.HiDensityCacheStorageInfo;
import com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecordStore;
import com.hazelcast.cache.hidensity.impl.nativememory.HotRestartHiDensityNativeMemoryCacheRecordStore;
import com.hazelcast.cache.hidensity.operation.CacheReplicationOperation;
import com.hazelcast.cache.hidensity.operation.CacheSegmentShutdownOperation;
import com.hazelcast.cache.hidensity.operation.HiDensityCacheOperationProvider;
import com.hazelcast.cache.hotrestart.HotRestartEnterpriseCacheRecordStore;
import com.hazelcast.cache.impl.CacheContext;
import com.hazelcast.cache.impl.CacheEventContext;
import com.hazelcast.cache.impl.CacheEventType;
import com.hazelcast.cache.impl.CacheOperationProvider;
import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.event.CacheWanEventPublisher;
import com.hazelcast.cache.impl.event.CacheWanEventPublisherImpl;
import com.hazelcast.cache.impl.merge.entry.DefaultCacheEntryView;
import com.hazelcast.cache.impl.merge.entry.LazyCacheEntryView;
import com.hazelcast.cache.impl.merge.policy.CacheMergePolicyProvider;
import com.hazelcast.cache.impl.wan.CacheFilterProvider;
import com.hazelcast.cache.operation.CacheDestroyOperation;
import com.hazelcast.cache.operation.EnterpriseCacheOperationProvider;
import com.hazelcast.cache.operation.WANAwareCacheOperationProvider;
import com.hazelcast.cache.wan.CacheReplicationRemove;
import com.hazelcast.cache.wan.CacheReplicationSupportingService;
import com.hazelcast.cache.wan.CacheReplicationUpdate;
import com.hazelcast.cache.wan.filter.CacheWanEventFilter;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.enterprise.wan.WanFilterEventType;
import com.hazelcast.instance.EnterpriseNodeExtension;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.spi.hotrestart.HotRestartService;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.RamStoreRegistry;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;
import com.hazelcast.wan.WanReplicationService;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.spi.hotrestart.PersistentCacheDescriptors.toPartitionId;

/**
 * The {@link ICacheService} implementation specified for enterprise usage.
 * This {@link EnterpriseCacheService} implementation mainly handles
 * <ul> <li>
 * {@link ICacheRecordStore} creation of caches with specified partition id
 * </li> <li>
 * Destroying segments and caches
 * </li> <li>
 * Mediating for cache events and listeners
 * </li> </ul>
 *
 * @author mdogan 05/02/14
 */
@SuppressWarnings("checkstyle:methodcount")
public class EnterpriseCacheService
        extends CacheService
        implements ReplicationSupportingService, RamStoreRegistry {

    private static final int CACHE_SEGMENT_DESTROY_OPERATION_AWAIT_TIME_IN_SECS = 30;

    protected final ConcurrentMap<String, WanReplicationPublisher> wanReplicationPublishers =
            new ConcurrentHashMap<String, WanReplicationPublisher>();
    protected final ConcurrentMap<String, String> cacheMergePolicies =
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
                    return new HiDensityCacheStorageInfo(cacheNameWithPrefix, cacheContext);
                }
            };

    private ReplicationSupportingService replicationSupportingService;
    private CacheMergePolicyProvider cacheMergePolicyProvider;
    private CacheFilterProvider cacheFilterProvider;
    private CacheWanEventPublisher cacheWanEventPublisher;
    private HotRestartService hotRestartService;

    @Override
    protected void postInit(NodeEngine nodeEngine, Properties properties) {
        super.postInit(nodeEngine, properties);
        replicationSupportingService = new CacheReplicationSupportingService(this);
        cacheMergePolicyProvider = new CacheMergePolicyProvider(nodeEngine);
        cacheFilterProvider = new CacheFilterProvider(nodeEngine);
        cacheWanEventPublisher = new CacheWanEventPublisherImpl(this);

        hotRestartService = getHotRestartService();
        if (hotRestartService != null) {
            hotRestartService.registerRamStoreRegistry(SERVICE_NAME, this);
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

    public HotRestartStore onHeapHotRestartStoreForCurrentThread() {
        return hotRestartService.getOnHeapHotRestartStoreForCurrentThread();
    }

    public HotRestartStore offHeapHotRestartStoreForCurrentThread() {
        return hotRestartService.getOffHeapHotRestartStoreForCurrentThread();
    }

    /**
     * Creates new {@link ICacheRecordStore} as specified {@link InMemoryFormat}.
     *
     * @param name        the name of the cache, including prefix
     * @param partitionId the partition id which cache record store is created on
     * @return the created {@link ICacheRecordStore}
     * @see com.hazelcast.cache.impl.CacheRecordStore
     * @see com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecordStore
     */
    @Override
    protected ICacheRecordStore createNewRecordStore(String name, int partitionId) {
        CacheConfig cacheConfig = getCacheConfig(name);
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

            hotRestartService.ensureHasConfiguration(SERVICE_NAME, name, cacheConfig);
            prefix = hotRestartService.registerRamStore(this, SERVICE_NAME, name, partitionId);
        }
        return isNative
                ? newNativeRecordStore(name, partitionId, hotRestartConfig, prefix)
                : newHeapRecordStore(name, partitionId, hotRestartConfig, prefix);
    }

    private static HotRestartConfig getHotRestartConfig(CacheConfig cacheConfig) {
        HotRestartConfig hotRestartConfig = cacheConfig.getHotRestartConfig();
        return hotRestartConfig != null ? hotRestartConfig : new HotRestartConfig().setEnabled(false);
    }

    private HotRestartService getHotRestartService() {
        NodeEngineImpl nodeEngineImpl = (NodeEngineImpl) nodeEngine;
        Node node = nodeEngineImpl.getNode();
        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) node.getNodeExtension();
        return nodeExtension.isHotRestartEnabled() ? nodeExtension.getHotRestartService() : null;
    }

    private ICacheRecordStore newHeapRecordStore(String name, int partitionId, HotRestartConfig hotRestart, long prefix) {
        return hotRestart.isEnabled()
                ? new HotRestartEnterpriseCacheRecordStore(name, partitionId, nodeEngine, this, hotRestart.isFsync(), prefix)
                : new DefaultEnterpriseCacheRecordStore(name, partitionId, nodeEngine, this);
    }

    private ICacheRecordStore newNativeRecordStore(String name, int partitionId, HotRestartConfig hotRestart, long prefix) {
        try {
            return hotRestart.isEnabled()
                    ? new HotRestartHiDensityNativeMemoryCacheRecordStore(partitionId, name, this, nodeEngine,
                    hotRestart.isFsync(), prefix)
                    : new HiDensityNativeMemoryCacheRecordStore(partitionId, name, this, nodeEngine);
        } catch (NativeOutOfMemoryError e) {
            throw new NativeOutOfMemoryError("Cannot create internal cache map, "
                    + "not enough contiguous memory available! -> " + e.getMessage(), e);
        }
    }

    @Override
    public CacheConfig getCacheConfig(String name) {
        CacheConfig cacheConfig = configs.get(name);
        if (cacheConfig == null && hotRestartService != null) {
            cacheConfig = hotRestartService.getProvisionalConfiguration(SERVICE_NAME, name);
        }
        return cacheConfig;
    }

    /**
     * Destroys the segments for specified cache name.
     *
     * @param cacheName the name of cache whose segments will be destroyed
     */
    @Override
    protected void destroySegments(String cacheName) {
        OperationService operationService = nodeEngine.getOperationService();
        //List<CacheDestroyOperation> ops = new ArrayList<CacheDestroyOperation>();
        for (CachePartitionSegment segment : segments) {
            if (segment.hasRecordStore(cacheName)) {
                CacheDestroyOperation op = new CacheDestroyOperation(cacheName);
                //ops.add(op);
                op.setPartitionId(segment.getPartitionId());
                op.setNodeEngine(nodeEngine).setService(this);
                operationService.executeOperation(op);
            }
        }
        hiDensityCacheInfoMap.remove(cacheName);
        // TODO This is commented-out since
        // there is a deadlock between HiDensity cache destroy and open-source destroy operations
        // Currently operations are fire and forget :)
        /*
        for (CacheDestroyOperation op : ops) {
            try {
                op.awaitCompletion(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        */
    }

    /**
     * Destroys the distributed object for specified <code>object name/cache name</code>.
     *
     * @param objectName the name of object/cache to be destroyed
     */
    @Override
    public void destroyDistributedObject(String objectName) {
        destroySegments(objectName);
    }

    /**
     * Shutdowns the cache service and destroy the caches with their segments.
     *
     * @param terminate condition about cache service will be closed or not
     */
    @Override
    public void shutdown(boolean terminate) {
        InternalOperationService operationService = (InternalOperationService) nodeEngine.getOperationService();
        List<CacheSegmentShutdownOperation> ops = new ArrayList<CacheSegmentShutdownOperation>();
        for (CachePartitionSegment segment : segments) {
            if (segment.hasAnyRecordStore()) {
                CacheSegmentShutdownOperation op = new CacheSegmentShutdownOperation();
                op.setPartitionId(segment.getPartitionId());
                op.setNodeEngine(nodeEngine).setService(this);

                if (operationService.isRunAllowed(op)) {
                    operationService.runOperationOnCallingThread(op);
                } else {
                    operationService.executeOperation(op);
                    ops.add(op);
                }
            }
        }
        for (CacheSegmentShutdownOperation op : ops) {
            try {
                op.awaitCompletion(CACHE_SEGMENT_DESTROY_OPERATION_AWAIT_TIME_IN_SECS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                nodeEngine.getLogger(getClass()).warning(e);
            }
        }
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
     * @param originalPartitionId the partition id of the record store stores the records of cache
     * @return the number of evicted records
     */
    public int forceEvict(String name, int originalPartitionId) {
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
        InternalOperationService operationService = (InternalOperationService) nodeEngine.getOperationService();
        return operationService.getPartitionThreadCount();
    }

    /**
     * Does forced eviction on other caches. Runs on the operation threads.
     *
     * @param name                the name of the cache not to be evicted
     * @param originalPartitionId the partition id of the record store stores the records of cache
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
     * Does forced eviction on all caches. Runs on the operation threads.
     *
     * @param originalPartitionId the partition id of the record store stores the records of cache
     * @return the number of evicted records
     */
    public int forceEvictOnAll(int originalPartitionId) {
        int evicted = 0;
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        int threadCount = getPartitionThreadCount();
        int mod = originalPartitionId % threadCount;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            if (partitionId % threadCount == mod) {
                for (CacheConfig cacheConfig : getCacheConfigs()) {
                    String cacheName = cacheConfig.getNameWithPrefix();
                    ICacheRecordStore cache = getRecordStore(cacheName, partitionId);
                    if (cache instanceof HiDensityCacheRecordStore) {
                        evicted += ((HiDensityCacheRecordStore) cache).forceEvict();
                    }
                }
            }
        }
        return evicted;
    }

    /**
     * Clears all record stores on the partitions owned by partition thread of original partition.
     *
     * @param originalPartitionId the id of original partition
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
                    }
                }
            }
        }
    }

    /**
     * Creates a {@link com.hazelcast.cache.hidensity.operation.CacheReplicationOperation} to start the replication.
     *
     * @param event the {@link PartitionReplicationEvent} holds the <code>partitionId</code>
     *              and <code>replica index</code>
     * @return the created {@link com.hazelcast.cache.hidensity.operation.CacheReplicationOperation}
     */
    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        CachePartitionSegment segment = segments[event.getPartitionId()];
        CacheReplicationOperation op =
                new CacheReplicationOperation(segment, event.getReplicaIndex());
        return op.isEmpty() ? null : op;
    }

    /**
     * Creates a {@link CacheOperationProvider} as specified {@link InMemoryFormat}
     * for specified <code>cacheNameWithPrefix</code>
     *
     * @param cacheNameWithPrefix the name of the cache (including prefix) that operation works on
     * @param inMemoryFormat      the format of memory such as <code>BINARY</code>, <code>OBJECT</code>
     *                            or <code>NATIVE</code>
     * @return
     */
    @Override
    protected CacheOperationProvider createOperationProvider(String cacheNameWithPrefix,
                                                             InMemoryFormat inMemoryFormat) {
        EnterpriseCacheOperationProvider operationProvider;
        if (InMemoryFormat.NATIVE.equals(inMemoryFormat)) {
            operationProvider = new HiDensityCacheOperationProvider(cacheNameWithPrefix);
        } else {
            operationProvider = new EnterpriseCacheOperationProvider(cacheNameWithPrefix);
        }

        if (isWanReplicationEnabled(cacheNameWithPrefix)) {
            return new WANAwareCacheOperationProvider(cacheNameWithPrefix,
                    operationProvider, wanReplicationPublishers.get(cacheNameWithPrefix));
        } else {
            return operationProvider;
        }
    }

    @Override
    public CacheOperationProvider getCacheOperationProvider(String nameWithPrefix, InMemoryFormat inMemoryFormat) {
        CacheOperationProvider cacheOperationProvider = operationProviderCache.get(nameWithPrefix);
        if (cacheOperationProvider != null) {
            return cacheOperationProvider;
        }
        cacheOperationProvider = createOperationProvider(nameWithPrefix, inMemoryFormat);
        CacheOperationProvider current = operationProviderCache.putIfAbsent(nameWithPrefix, cacheOperationProvider);
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
    public CacheConfig putCacheConfigIfAbsent(CacheConfig config) {
        CacheConfig localConfig = super.putCacheConfigIfAbsent(config);
        if (localConfig != null) {
            config = localConfig;
        }
        WanReplicationRef wanReplicationRef = config.getWanReplicationRef();
        if (wanReplicationRef != null) {
            WanReplicationService wanReplicationService = nodeEngine.getWanReplicationService();
            wanReplicationPublishers.putIfAbsent(
                    config.getNameWithPrefix(),
                    wanReplicationService.getWanReplicationPublisher(wanReplicationRef.getName()));
            cacheMergePolicies.putIfAbsent(config.getNameWithPrefix(), wanReplicationRef.getMergePolicy());
        }
        return localConfig;
    }

    @Override
    public CacheConfig deleteCacheConfig(String name) {
        wanReplicationPublishers.remove(name);
        return super.deleteCacheConfig(name);
    }

    public CacheMergePolicyProvider getCacheMergePolicyProvider() {
        return cacheMergePolicyProvider;
    }

    @Override
    public void onReplicationEvent(WanReplicationEvent wanReplicationEvent) {
        replicationSupportingService.onReplicationEvent(wanReplicationEvent);
    }

    public void publishWanEvent(CacheEventContext cacheEventContext) {
        publishWanEvent(cacheEventContext, false);
    }

    public void publishWanEventBackup(CacheEventContext cacheEventContext) {
        publishWanEvent(cacheEventContext, true);
    }

    private void publishWanEvent(CacheEventContext cacheEventContext, boolean backup) {
        String cacheName = cacheEventContext.getCacheName();
        CacheEventType eventType = cacheEventContext.getEventType();
        WanReplicationPublisher wanReplicationPublisher =
                wanReplicationPublishers.get(cacheEventContext.getCacheName());
        if (wanReplicationPublisher != null && cacheEventContext.getOrigin() == null) {
            CacheConfig config = configs.get(cacheName);
            List<String> filters = config.getWanReplicationRef().getFilters();

            if (isEventFiltered(cacheEventContext, filters)) {
                return;
            }

            if (eventType == CacheEventType.UPDATED
                    || eventType == CacheEventType.CREATED
                    || eventType == CacheEventType.EXPIRATION_TIME_UPDATED) {
                CacheReplicationUpdate update =
                        new CacheReplicationUpdate(
                                config.getName(),
                                cacheMergePolicies.get(cacheName),
                                new DefaultCacheEntryView(cacheEventContext.getDataKey(),
                                        cacheEventContext.getDataValue(),
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
                        Clock.currentTimeMillis(), config.getManagerPrefix(), config.getTotalBackupCount());
                if (backup) {
                    wanReplicationPublisher.publishReplicationEventBackup(SERVICE_NAME, remove);
                } else {
                    wanReplicationPublisher.publishReplicationEvent(SERVICE_NAME, remove);
                }
            }
        }
    }

    private boolean isEventFiltered(CacheEventContext eventContext, List<String> filters) {
        if (!filters.isEmpty()) {
            CacheEntryView entryView = new LazyCacheEntryView(eventContext.getDataKey(),
                    eventContext.getDataValue(), eventContext.getExpirationTime(), eventContext.getLastAccessTime(),
                    eventContext.getAccessHit(), getSerializationService());
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

    public void publishWanEvent(String cacheName, WanReplicationEvent wanReplicationEvent) {
        WanReplicationPublisher wanReplicationPublisher = wanReplicationPublishers.get(cacheName);
        if (wanReplicationPublisher != null) {
            wanReplicationPublisher.publishReplicationEvent(wanReplicationEvent);
        }
    }

    @Override
    public boolean isWanReplicationEnabled(String cacheName) {
        WanReplicationPublisher publisher = wanReplicationPublishers.get(cacheName);
        return publisher != null;
    }

    @Override
    public CacheWanEventPublisher getCacheWanEventPublisher() {
        return cacheWanEventPublisher;
    }
}
