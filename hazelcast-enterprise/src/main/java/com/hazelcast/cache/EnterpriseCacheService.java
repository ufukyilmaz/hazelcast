package com.hazelcast.cache;

import com.hazelcast.cache.hidensity.HiDensityCacheInfo;
import com.hazelcast.cache.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.hidensity.client.CacheInvalidationListener;
import com.hazelcast.cache.hidensity.client.CacheInvalidationMessage;
import com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecordStore;
import com.hazelcast.cache.hidensity.operation.HiDensityCacheOperationProvider;
import com.hazelcast.cache.hidensity.operation.HiDensityCacheReplicationOperation;
import com.hazelcast.cache.hidensity.operation.CacheDestroyOperation;
import com.hazelcast.cache.hidensity.operation.CacheSegmentDestroyOperation;
import com.hazelcast.cache.impl.CacheEventType;
import com.hazelcast.cache.impl.CacheOperationProvider;
import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.merge.CacheMergePolicyProvider;
import com.hazelcast.cache.operation.EnterpriseCacheOperationProvider;
import com.hazelcast.cache.wan.CacheReplicationRemove;
import com.hazelcast.cache.wan.CacheReplicationSupportingService;
import com.hazelcast.cache.wan.CacheReplicationUpdate;
import com.hazelcast.cache.wan.SimpleCacheEntryView;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;
import com.hazelcast.wan.WanReplicationService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * The {@link CacheService} implementation specified for enterprise usage.
 * This {@link EnterpriseCacheService} implementation mainly handles
 * <ul>
 * <li>
 * {@link ICacheRecordStore} creation of caches with specified partition id
 * </li>
 * <li>
 * Destroying segments and caches
 * </li>
 * <li>
 * Mediating for cache events and listeners
 * </li>
 * </ul>
 *
 * @author mdogan 05/02/14
 */
public class EnterpriseCacheService extends CacheService implements ReplicationSupportingService {

    private static final int CACHE_SEGMENT_DESTROY_OPERATION_AWAIT_TIME_IN_SECS = 30;

    protected final ConcurrentMap<String, WanReplicationPublisher> wanReplicationPublishers =
            new ConcurrentHashMap<String, WanReplicationPublisher>();
    protected final ConcurrentMap<String, String> cacheMergePolicies =
            new ConcurrentHashMap<String, String>();

    private final ConcurrentMap<String, HiDensityCacheInfo> hiDensityCacheInfoMap =
            new ConcurrentHashMap<String, HiDensityCacheInfo>();
    private final ConstructorFunction<String, HiDensityCacheInfo> hiDensityCacheInfoConstructorFunction =
            new ConstructorFunction<String, HiDensityCacheInfo>() {
                @Override
                public HiDensityCacheInfo createNew(String cacheNameWithPrefix) {
                    return new HiDensityCacheInfo(cacheNameWithPrefix);
                }
            };
    private ReplicationSupportingService replicationSupportingService;
    private CacheMergePolicyProvider cacheMergePolicyProvider;

    @Override
    protected void postInit(NodeEngine nodeEngine, Properties properties) {
        replicationSupportingService = new CacheReplicationSupportingService(this);
        cacheMergePolicyProvider = new CacheMergePolicyProvider(nodeEngine);
    }

    /**
     * Creates new {@link ICacheRecordStore} as specified {@link InMemoryFormat}.
     *
     * @param name        the name of the cache with prefix
     * @param partitionId the partition id which cache record store is created on
     * @return the created {@link ICacheRecordStore}
     *
     * @see com.hazelcast.cache.impl.CacheRecordStore
     * @see com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecordStore
     */
    @Override
    protected ICacheRecordStore createNewRecordStore(String name, int partitionId) {
        CacheConfig cacheConfig = configs.get(name);
        if (cacheConfig == null) {
            throw new IllegalArgumentException("CacheConfig is null! " + name);
        }
        InMemoryFormat inMemoryFormat = cacheConfig.getInMemoryFormat();
        if (InMemoryFormat.NATIVE.equals(inMemoryFormat)) {
            try {
                return new HiDensityNativeMemoryCacheRecordStore(partitionId, name, this, nodeEngine);
            } catch (NativeOutOfMemoryError e) {
                throw new NativeOutOfMemoryError("Cannot create internal cache map, "
                        + "not enough contiguous memory available! -> " + e.getMessage(), e);
            }
        } else if (inMemoryFormat == null
                || InMemoryFormat.BINARY.equals(inMemoryFormat)
                || InMemoryFormat.OBJECT.equals(inMemoryFormat)) {
            return new EnterpriseCacheRecordStoreImpl(name, partitionId, nodeEngine, this);
        }

        throw new IllegalArgumentException("Cannot create record store for the storage type: "
                + inMemoryFormat);
    }



    /**
     * Destroys the segments for specified <code>object name/cache name</code>.
     *
     * @param objectName the name of object/cache whose segments will be destroyed
     */
    @Override
    protected void destroySegments(String objectName) {
        OperationService operationService = nodeEngine.getOperationService();
        List<CacheDestroyOperation> ops = new ArrayList<CacheDestroyOperation>();
        for (CachePartitionSegment segment : segments) {
            if (segment.hasCache(objectName)) {
                CacheDestroyOperation op = new CacheDestroyOperation(objectName);
                ops.add(op);
                op.setPartitionId(segment.getPartitionId())
                        .setNodeEngine(nodeEngine).setService(this);
                operationService.executeOperation(op);
            }
        }
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
        OperationService operationService = nodeEngine.getOperationService();
        List<CacheSegmentDestroyOperation> ops = new ArrayList<CacheSegmentDestroyOperation>();
        for (CachePartitionSegment segment : segments) {
            if (segment.hasAnyCache()) {
                CacheSegmentDestroyOperation op = new CacheSegmentDestroyOperation();
                op.setPartitionId(segment.getPartitionId())
                        .setNodeEngine(nodeEngine).setService(this);

                if (operationService.isAllowedToRunOnCallingThread(op)) {
                    operationService.runOperationOnCallingThread(op);
                } else {
                    operationService.executeOperation(op);
                    ops.add(op);
                }
            }
        }
        for (CacheSegmentDestroyOperation op : ops) {
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
        int threadCount = nodeEngine.getOperationService().getPartitionOperationThreadCount();
        int mod = originalPartitionId % threadCount;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            if (partitionId % threadCount == mod) {
                ICacheRecordStore cache = getCacheRecordStore(name, partitionId);
                if (cache instanceof HiDensityCacheRecordStore) {
                    evicted += ((HiDensityCacheRecordStore) cache).forceEvict();
                }
            }
        }
        return evicted;
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
        int threadCount = nodeEngine.getOperationService().getPartitionOperationThreadCount();
        int mod = originalPartitionId % threadCount;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            if (partitionId % threadCount == mod) {
                for (CacheConfig cacheConfig : getCacheConfigs()) {
                    String cacheName = cacheConfig.getNameWithPrefix();
                    if (!cacheName.equals(name)) {
                        ICacheRecordStore cache = getCacheRecordStore(cacheName, partitionId);
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
        int threadCount = nodeEngine.getOperationService().getPartitionOperationThreadCount();
        int mod = originalPartitionId % threadCount;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            if (partitionId % threadCount == mod) {
                for (CacheConfig cacheConfig : getCacheConfigs()) {
                    String cacheName = cacheConfig.getNameWithPrefix();
                    ICacheRecordStore cache = getCacheRecordStore(cacheName, partitionId);
                    if (cache instanceof HiDensityCacheRecordStore) {
                        evicted += ((HiDensityCacheRecordStore) cache).forceEvict();
                    }
                }
            }
        }
        return evicted;
    }

    /**
     * Creates a {@link com.hazelcast.cache.hidensity.operation.HiDensityCacheReplicationOperation} to start the replication.
     *
     * @param event the {@link PartitionReplicationEvent} holds the <code>partitionId</code>
     *              and <code>replica index</code>
     * @return the created {@link com.hazelcast.cache.hidensity.operation.HiDensityCacheReplicationOperation}
     */
    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        CachePartitionSegment segment = segments[event.getPartitionId()];
        HiDensityCacheReplicationOperation op =
                new HiDensityCacheReplicationOperation(segment, event.getReplicaIndex());
        return op.isEmpty() ? null : op;
    }

    /**
     * Registers and {@link CacheInvalidationListener} for specified <code>cacheName</code>.
     *
     * @param cacheName the name of the cache that {@link CacheInvalidationListener} will be registered for
     * @param listener  the {@link CacheInvalidationListener} to be registered for specified <code>cache</code>
     * @return the id which is unique for current registration
     */
    public String addInvalidationListener(String cacheName, CacheInvalidationListener listener) {
        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration =
                eventService.registerLocalListener(SERVICE_NAME, cacheName, listener);
        return registration.getId();
    }

    /**
     * Sends an invalidation event for given <code>cacheName</code> with specified <code>key</code>
     * from mentioned source with <code>sourceUuid</code>.
     *
     * @param cacheName  the name of the cache that invalidation event is sent for
     * @param key        the {@link Data} represents the invalidation event
     * @param sourceUuid an id that represents the source for invalidation event
     */
    public void sendInvalidationEvent(String cacheName, Data key, String sourceUuid) {
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations =
                eventService.getRegistrations(SERVICE_NAME, cacheName);
        if (!registrations.isEmpty()) {
            EnterpriseSerializationService ss = getSerializationService();
            Data event = ss.convertData(key, DataType.HEAP);
            eventService.publishEvent(SERVICE_NAME, registrations,
                    new CacheInvalidationMessage(cacheName, event, sourceUuid), cacheName.hashCode());
        }
    }

    /**
     * Creates a {@link CacheOperationProvider} as specified {@link InMemoryFormat}
     * for specified <code>cacheNameWithPrefix</code>
     *
     * @param cacheNameWithPrefix the name of the cache with prefix that operation works on
     * @param inMemoryFormat      the format of memory such as <code>BINARY</code>, <code>OBJECT</code>
     *                            or <code>OFFHEAP</code>
     * @return
     */
    @Override
    public CacheOperationProvider getCacheOperationProvider(String cacheNameWithPrefix,
                                                            InMemoryFormat inMemoryFormat) {
        if (InMemoryFormat.NATIVE.equals(inMemoryFormat)) {
            return new HiDensityCacheOperationProvider(cacheNameWithPrefix);
        }
        return new EnterpriseCacheOperationProvider(cacheNameWithPrefix);
    }

    /**
     * Gets the {@link EnterpriseSerializationService} used by this {@link CacheService}.
     *
     * @return the used {@link EnterpriseSerializationService}
     */
    public EnterpriseSerializationService getSerializationService() {
        return (EnterpriseSerializationService) nodeEngine.getSerializationService();
    }

    /**
     * Gets or creates (if there is no cache info for that Hi-Density cache) {@link HiDensityCacheInfo} instance
     * which holds live information about cache.
     *
     * @param cacheNameWithPrefix Name (with prefix) of the cache whose live information is requested
     *
     * @return the {@link HiDensityCacheInfo} instance which holds live information about Hi-Density cache
     */
    public HiDensityCacheInfo getOrCreateHiDensityCacheInfo(String cacheNameWithPrefix) {
        return ConcurrencyUtil.getOrPutSynchronized(hiDensityCacheInfoMap, cacheNameWithPrefix,
                this, hiDensityCacheInfoConstructorFunction);
    }

    @Override
    public CacheConfig createCacheConfigIfAbsent(CacheConfig config) {
        CacheConfig localConfig = super.createCacheConfigIfAbsent(config);
        if (localConfig != null) {
            config = localConfig;
        }
        WanReplicationRef wanReplicationRef = config.getWanReplicationRef();
        if (wanReplicationRef != null) {
            WanReplicationService wanReplicationService = nodeEngine.getWanReplicationService();
            wanReplicationPublishers.putIfAbsent(config.getNameWithPrefix(),
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

    @Override
    public void publishEvent(String cacheName, CacheEventType eventType, Data dataKey, Data dataValue,
                             Data dataOldValue, boolean isOldValueAvailable,
                             int orderKey, int completionId, long expirationTime,
                             String origin) {
        WanReplicationPublisher wanReplicationPublisher = wanReplicationPublishers.get(cacheName);

        if (wanReplicationPublisher != null
                && origin == null) {

            String groupName = nodeEngine.getConfig().getGroupConfig().getName();
            CacheConfig config = configs.get(cacheName);
            if (eventType == CacheEventType.UPDATED
                    || eventType == CacheEventType.CREATED
                    || eventType == CacheEventType.EXPIRATION_TIME_UPDATED) {
                CacheReplicationUpdate update =
                        new CacheReplicationUpdate(config.getName(), cacheMergePolicies.get(cacheName),
                                new SimpleCacheEntryView(dataKey, dataValue, expirationTime),
                                groupName, config.getUriString());
                wanReplicationPublisher.publishReplicationEvent(SERVICE_NAME, update);
            } else if (eventType == CacheEventType.REMOVED) {
                CacheReplicationRemove remove = new CacheReplicationRemove(config.getName(), dataKey,
                        Clock.currentTimeMillis(), groupName, config.getUriString());
                wanReplicationPublisher.publishReplicationEvent(SERVICE_NAME, remove);
            }
        }

        super.publishEvent(cacheName, eventType, dataKey, dataValue, dataOldValue,
                isOldValueAvailable, orderKey, completionId, expirationTime, origin);
    }

    public void publishWanEvent(String cacheName, ReplicationEventObject replicationEventObject) {
        WanReplicationPublisher wanReplicationPublisher = wanReplicationPublishers.get(cacheName);
        if (wanReplicationPublisher != null) {
            wanReplicationPublisher.publishReplicationEvent(SERVICE_NAME, replicationEventObject);
        }
    }

    @Override
    public String toString() {
        return "EnterpriseCacheService[" + SERVICE_NAME + "]";
    }

}
