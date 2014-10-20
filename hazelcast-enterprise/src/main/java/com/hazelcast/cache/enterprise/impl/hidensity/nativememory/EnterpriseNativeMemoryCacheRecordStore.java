package com.hazelcast.cache.enterprise.impl.hidensity.nativememory;

import com.hazelcast.cache.enterprise.*;
import com.hazelcast.cache.enterprise.impl.hidensity.AbstractEnterpriseHiDensityCacheRecordStore;
import com.hazelcast.cache.enterprise.operation.CacheEvictionOperation;
import com.hazelcast.cache.impl.*;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.elasticcollections.map.BinaryOffHeapHashMap;
import com.hazelcast.map.impl.MapEntrySet;
import com.hazelcast.memory.MemoryBlockAccessor;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.error.OffHeapOutOfMemoryError;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.EmptyStatement;

import javax.cache.configuration.Factory;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessor;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.impl.record.CacheRecordFactory.isExpiredAt;

/**
 * @author sozal 14/10/14
 */
public class EnterpriseNativeMemoryCacheRecordStore
        extends AbstractEnterpriseHiDensityCacheRecordStore {

    public static final int DEFAULT_INITIAL_CAPACITY = 1000;
    public static final long NULL_PTR = MemoryManager.NULL_ADDRESS;

    protected final int partitionId;
    protected final NodeEngine nodeEngine;
    protected final CacheConfig cacheConfig;
    protected final EnterpriseNativeMemoryCacheHashMap records;
    protected final EnterpriseCacheService cacheService;
    protected final EnterpriseSerializationService serializationService;
    protected final ScheduledFuture<?> evictionTaskFuture;
    protected final Operation evictionOperation;
    protected final MemoryManager memoryManager;
    protected final CacheRecordAccessor cacheRecordService;
    protected final EvictionPolicy evictionPolicy;
    protected final ExpiryPolicy expiryPolicy;
    protected final boolean evictionEnabled;
    protected final int evictionPercentage;
    protected final float evictionThreshold;
    protected boolean hasExpiringEntry;
    protected boolean isEventsEnabled = true;
    protected boolean isEventBatchingEnabled;
    protected final Map<CacheEventType, Set<CacheEventData>> batchEvent =
            new HashMap<CacheEventType, Set<CacheEventData>>();
    protected CacheStatisticsImpl statistics;
    protected CacheLoader cacheLoader;
    protected CacheWriter cacheWriter;
    protected volatile boolean hasTtl;
    protected final long defaultTTL;

    protected EnterpriseNativeMemoryCacheRecordStore(final int partitionId,
                                                     final CacheConfig cacheConfig,
                                                     final EnterpriseCacheService cacheService,
                                                     final EnterpriseSerializationService serializationService,
                                                     final NodeEngine nodeEngine,
                                                     final int initialCapacity,
                                                     final ExpiryPolicy expiryPolicy,
                                                     final EvictionPolicy evictionPolicy,
                                                     final int evictionPercentage,
                                                     final int evictionThresholdPercentage) {
        if (cacheConfig == null) {
            throw new IllegalStateException("Cache already destroyed");
        }
        this.partitionId = partitionId;
        this.cacheConfig = cacheConfig;
        this.cacheService = cacheService;
        this.serializationService = serializationService;
        this.nodeEngine = nodeEngine;
        this.expiryPolicy = expiryPolicy != null ? expiryPolicy : (ExpiryPolicy)cacheConfig.getExpiryPolicyFactory().create();
        this.evictionPolicy = evictionPolicy != null ? evictionPolicy : cacheConfig.getEvictionPolicy();
        this.cacheRecordService = new CacheRecordAccessor(serializationService);
        this.records = new EnterpriseNativeMemoryCacheHashMap(initialCapacity, serializationService, cacheRecordService, createEvictionCallback());
        this.memoryManager = serializationService.getMemoryManager();
        this.evictionEnabled = evictionPolicy != EvictionPolicy.NONE;
        this.evictionPercentage = evictionPercentage;
        this.evictionThreshold = (float) Math.max(1, 100 - evictionThresholdPercentage) / 100;
        if (cacheConfig.getCacheLoaderFactory() != null) {
            final Factory<CacheLoader> cacheLoaderFactory = cacheConfig.getCacheLoaderFactory();
            cacheLoader = cacheLoaderFactory.create();
        }
        if (cacheConfig.getCacheWriterFactory() != null) {
            final Factory<CacheWriter> cacheWriterFactory = cacheConfig.getCacheWriterFactory();
            cacheWriter = cacheWriterFactory.create();
        }
        long ttl = expiryPolicyToTTL(expiryPolicy);
        this.defaultTTL = ttl > 0 ? ttl : DEFAULT_TTL;
        this.hasTtl = defaultTTL > 0;
        this.evictionOperation = createEvictionOperation(10);
        this.evictionTaskFuture =
                nodeEngine.getExecutionService()
                    .scheduleWithFixedDelay("hz:cache", new EvictionTask(), 5, 5, TimeUnit.SECONDS);
    }

    public EnterpriseNativeMemoryCacheRecordStore(final int partitionId,
                                                  final String cacheName,
                                                  final EnterpriseCacheService cacheService,
                                                  final EnterpriseSerializationService ss,
                                                  final NodeEngine nodeEngine,
                                                  final int initialCapacity) {
        this(partitionId,
             cacheService.getCacheConfig(cacheName),
             cacheService,
             ss,
             nodeEngine,
             initialCapacity,
             null,
             null,
             DEFAULT_EVICTION_PERCENTAGE,
             DEFAULT_EVICTION_THRESHOLD_PERCENTAGE);
    }

    public EnterpriseNativeMemoryCacheRecordStore(final int partitionId,
                                                  final CacheConfig cacheConfig,
                                                  final EnterpriseCacheService cacheService,
                                                  final EnterpriseSerializationService ss,
                                                  final NodeEngine nodeEngine,
                                                  final int initialCapacity) {
        this(partitionId,
             cacheConfig,
             cacheService,
             ss,
             nodeEngine,
             initialCapacity,
             (ExpiryPolicy)cacheConfig.getExpiryPolicyFactory().create(),
             cacheConfig.getEvictionPolicy(),
             DEFAULT_EVICTION_PERCENTAGE,
             DEFAULT_EVICTION_THRESHOLD_PERCENTAGE);
    }

    protected boolean isStatisticsEnabled() {
        if (!cacheConfig.isStatisticsEnabled()) {
            return false;
        }
        if (statistics == null) {
            this.statistics = cacheService.createCacheStatIfAbsent(cacheConfig.getName());
        }
        return true;
    }

    protected boolean isReadThrough() {
        return cacheConfig.isReadThrough();
    }

    protected boolean isWriteThrough() {
        return cacheConfig.isWriteThrough();
    }

    protected OffHeapData getRecordData(EnterpriseNativeMemoryCacheRecord record) {
        return (OffHeapData) cacheRecordService.readData(record.getValueAddress());
    }

    protected Object getDataValue(OffHeapData offHeapData) {
        if (offHeapData != null) {
            return serializationService.convertData(offHeapData, DataType.HEAP);
        } else {
            return null;
        }
    }

    protected Object getRecordValue(EnterpriseNativeMemoryCacheRecord record) {
        return getDataValue(getRecordData(record));
    }

    public void evictExpiredRecords() {
        evictExpiredRecords(evictionPercentage);
    }

    public boolean createRecordWithExpiry(Data key,
                                          Object value,
                                          ExpiryPolicy localExpiryPolicy,
                                          long now,
                                          boolean disableWriteThrough) {
        Duration expiryDuration;
        try {
            expiryDuration = localExpiryPolicy.getExpiryForCreation();
        } catch (Exception e) {
            expiryDuration = Duration.ETERNAL;
        }
        long expiryTime = expiryDuration.getAdjustedTime(now);

        if (!disableWriteThrough) {
            writeThroughCache(key, value);
        }

        if (!isExpiredAt(expiryTime, now)) {
            EnterpriseNativeMemoryCacheRecord record = createRecord(key, value, expiryTime);
            records.put(key, record);
            return true;
        }
        return false;
    }

    public EnterpriseNativeMemoryCacheRecord createRecord(Data keyData,
                                                          Object value,
                                                          long expirationTime) {
        final EnterpriseNativeMemoryCacheRecord record =
                createRecord(value, Clock.currentTimeMillis(), expirationTime);
        if (isEventsEnabled) {
            final OffHeapData recordValue = record.getValue();
            publishEvent(cacheConfig.getName(),
                         CacheEventType.CREATED,
                         keyData,
                         null,
                         recordValue,
                         false);
        }
        return record;
    }

    public boolean updateRecordWithExpiry(Data key,
                                          Object value,
                                          EnterpriseNativeMemoryCacheRecord record,
                                          ExpiryPolicy localExpiryPolicy,
                                          long now,
                                          boolean disableWriteThrough) {
        long expiryTime = -1L;
        try {
            Duration expiryDuration = localExpiryPolicy.getExpiryForUpdate();
            if (expiryDuration != null) {
                expiryTime = expiryDuration.getAdjustedTime(now);
                record.setExpirationTime(expiryTime);
            }
        } catch (Exception e) {
            EmptyStatement.ignore(e);
            //leave the expiry time untouched when we can't determine a duration
        }
        if (!disableWriteThrough) {
            writeThroughCache(key, value);
        }
        updateRecord(key, record, value);
        return !processExpiredEntry(key, record, expiryTime, now);
    }

    public EnterpriseNativeMemoryCacheRecord updateRecord(Data key,
                                                          EnterpriseNativeMemoryCacheRecord record, Object value) {
        final OffHeapData dataOldValue = (OffHeapData) record.getValue();
        final OffHeapData dataValue = toOffHeapData(value);
        record.setValue(dataValue);
        if (isEventsEnabled) {
            publishEvent(cacheConfig.getName(),
                         CacheEventType.UPDATED,
                         key,
                         dataOldValue,
                         dataValue,
                         true);
        }
        return record;
    }

    public void deleteRecord(Data key) {
        final EnterpriseNativeMemoryCacheRecord record = records.remove(key);
        final OffHeapData dataValue = record.getValue();
        if (isEventsEnabled) {
            publishEvent(cacheConfig.getName(),
                         CacheEventType.REMOVED,
                         key,
                         null,
                         dataValue,
                         false);
        }
    }

    public EnterpriseNativeMemoryCacheRecord accessRecord(EnterpriseNativeMemoryCacheRecord record,
                                              ExpiryPolicy expiryPolicy,
                                              long now) {
        final ExpiryPolicy localExpiryPolicy =
                expiryPolicy != null
                        ? expiryPolicy
                        : this.expiryPolicy;
        updateAccessDuration(record, localExpiryPolicy, now);
        return record;
    }

    public EnterpriseNativeMemoryCacheRecord readThroughRecord(Data key, long now) {
        final ExpiryPolicy localExpiryPolicy = expiryPolicy;
        Object value = readThroughCache(key);
        if (value == null) {
            return null;
        }
        Duration expiryDuration;
        try {
            expiryDuration = localExpiryPolicy.getExpiryForCreation();
        } catch (Exception e) {
            expiryDuration = Duration.ETERNAL;
        }
        long expiryTime = expiryDuration.getAdjustedTime(now);

        if (isExpiredAt(expiryTime, now)) {
            return null;
        }
        return createRecord(key, value, expiryTime);
    }

    protected Object readThroughCache(Data key) throws CacheLoaderException {
        if (isReadThrough() && cacheLoader != null) {
            try {
                Object o = cacheService.toObject(key);
                return cacheLoader.load(o);
            } catch (Exception e) {
                if (!(e instanceof CacheLoaderException)) {
                    throw new CacheLoaderException("Exception in CacheLoader during load", e);
                } else {
                    throw (CacheLoaderException) e;
                }
            }
        }
        return null;
    }

    protected void writeThroughCache(Data key, Object value) throws CacheWriterException {
        if (isWriteThrough() && cacheWriter != null) {
            try {
                final Object objKey = cacheService.toObject(key);
                final Object objValue = cacheService.toObject(value);
                CacheEntry<?, ?> entry = new CacheEntry<Object, Object>(objKey, objValue);
                cacheWriter.write(entry);
            } catch (Exception e) {
                if (!(e instanceof CacheWriterException)) {
                    throw new CacheWriterException("Exception in CacheWriter during write", e);
                } else {
                    throw (CacheWriterException) e;
                }
            }
        }
    }

    protected void deleteCacheEntry(Data key) {
        if (isWriteThrough() && cacheWriter != null) {
            try {
                final Object objKey = cacheService.toObject(key);
                cacheWriter.delete(objKey);
            } catch (Exception e) {
                if (!(e instanceof CacheWriterException)) {
                    throw new CacheWriterException("Exception in CacheWriter during delete", e);
                } else {
                    throw (CacheWriterException) e;
                }
            }
        }
    }

    protected void deleteAllCacheEntry(Set<Data> keys) {
        if (isWriteThrough() && cacheWriter != null && keys != null && !keys.isEmpty()) {
            Map<Object, Data> keysToDelete = new HashMap<Object, Data>();
            for (Data key : keys) {
                final Object localKeyObj = cacheService.toObject(key);
                keysToDelete.put(localKeyObj, key);
            }
            final Set<Object> keysObject = keysToDelete.keySet();
            try {
                cacheWriter.deleteAll(keysObject);
            } catch (Exception e) {
                if (!(e instanceof CacheWriterException)) {
                    throw new CacheWriterException("Exception in CacheWriter during deleteAll", e);
                } else {
                    throw (CacheWriterException) e;
                }
            } finally {
                for (Object undeletedKey : keysObject) {
                    final Data undeletedKeyData = keysToDelete.get(undeletedKey);
                    keys.remove(undeletedKeyData);
                }
            }
        }
    }

    protected Map<Data, Object> loadAllCacheEntry(Set<Data> keys) {
        if (cacheLoader != null) {
            Map<Object, Data> keysToLoad = new HashMap<Object, Data>();
            for (Data key : keys) {
                final Object localKeyObj = cacheService.toObject(key);
                keysToLoad.put(localKeyObj, key);
            }

            Map<Object, Object> loaded;
            try {
                loaded = cacheLoader.loadAll(keysToLoad.keySet());
            } catch (Throwable e) {
                if (!(e instanceof CacheLoaderException)) {
                    throw new CacheLoaderException("Exception in CacheLoader during loadAll", e);
                } else {
                    throw (CacheLoaderException) e;
                }
            }
            Map<Data, Object> result = new HashMap<Data, Object>();

            for (Map.Entry<Object, Data> entry : keysToLoad.entrySet()) {
                final Object keyObj = entry.getKey();
                final Object valueObject = loaded.get(keyObj);
                final Data keyData = entry.getValue();
                result.put(keyData, valueObject);
            }
            return result;
        }
        return null;
    }

    protected boolean processExpiredEntry(Data key, EnterpriseNativeMemoryCacheRecord record, long now) {
        final boolean isExpired = record != null && record.isExpiredAt(now);
        if (!isExpired) {
            return false;
        }
        if (isStatisticsEnabled()) {
            statistics.increaseCacheExpiries(1);
        }
        records.remove(key);
        if (isEventsEnabled) {
            final Data dataValue = getRecordData(record);
            publishEvent(cacheConfig.getName(), CacheEventType.EXPIRED, key, null, dataValue, false);
        }
        return true;
    }

    protected boolean processExpiredEntry(Data key, EnterpriseNativeMemoryCacheRecord record, long expiryTime, long now) {
        final boolean isExpired = isExpiredAt(expiryTime, now);
        if (!isExpired) {
            return false;
        }
        if (isStatisticsEnabled()) {
            statistics.increaseCacheExpiries(1);
        }
        records.remove(key);
        if (isEventsEnabled) {
            final OffHeapData dataValue = getRecordData(record);
            publishEvent(cacheConfig.getName(), CacheEventType.EXPIRED, key, null, dataValue, false);
        }
        return true;
    }

    protected long updateAccessDuration(EnterpriseNativeMemoryCacheRecord record,
                                        ExpiryPolicy expiryPolicy,
                                        long now) {
        long expiryTime = -1L;
        try {
            Duration expiryDuration = expiryPolicy.getExpiryForAccess();
            if (expiryDuration != null) {
                expiryTime = expiryDuration.getAdjustedTime(now);
                record.setExpirationTime(expiryTime);
            }
        } catch (Exception e) {
            EmptyStatement.ignore(e);
            //leave the expiry time untouched when we can't determine a duration
        }
        return expiryTime;
    }

    protected void updateGetAndPutStat(boolean isPutSucceed,
                                       boolean getValue,
                                       boolean oldValueNull,
                                       long start) {
        if (isStatisticsEnabled()) {
            if (isPutSucceed) {
                statistics.increaseCachePuts(1);
                statistics.addPutTimeNano(System.nanoTime() - start);
            }
            if (getValue) {
                if (oldValueNull) {
                    statistics.increaseCacheMisses(1);
                } else {
                    statistics.increaseCacheHits(1);
                }
                statistics.addGetTimeNano(System.nanoTime() - start);
            }
        }
    }

    protected void updateReplaceStat(boolean result, boolean isHit, long start) {
        if (isStatisticsEnabled()) {
            if (result) {
                statistics.increaseCachePuts(1);
                statistics.addPutTimeNano(System.nanoTime() - start);
            }
            statistics.addGetTimeNano(System.nanoTime() - start);
            if (isHit) {
                statistics.increaseCacheHits(1);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
    }

    protected void publishEvent(String cacheName,
                                CacheEventType eventType,
                                Data dataKey,
                                Data dataOldValue,
                                Data dataValue,
                                boolean isOldValueAvailable) {
        if (isEventBatchingEnabled) {
            final CacheEventDataImpl cacheEventData = new CacheEventDataImpl(cacheName, eventType, dataKey, dataValue,
                    dataOldValue, isOldValueAvailable);
            Set<CacheEventData> cacheEventDatas = batchEvent.get(eventType);
            if (cacheEventDatas == null) {
                cacheEventDatas = new HashSet<CacheEventData>();
                batchEvent.put(eventType, cacheEventDatas);
            }
            cacheEventDatas.add(cacheEventData);
        } else {
            cacheService.publishEvent(cacheName,
                    eventType,
                    dataKey,
                    dataValue,
                    dataOldValue,
                    isOldValueAvailable,
                    dataKey.hashCode());
        }
    }

    protected void publishBatchedEvents(String cacheName,
                                        CacheEventType cacheEventType,
                                        int orderKey) {
        final Set<CacheEventData> cacheEventDatas = batchEvent.get(cacheEventType);
        CacheEventSet ces = new CacheEventSet(cacheEventType, cacheEventDatas);
        cacheService.publishEvent(cacheName, ces, orderKey);
    }

    protected void onAccess(long now, EnterpriseNativeMemoryCacheRecord record, long creationTime) {
        if (evictionEnabled) {
            if (evictionPolicy == EvictionPolicy.LRU) {
                long longDiff = now - creationTime;
                int diff = longDiff < Integer.MAX_VALUE ? (int) longDiff : Integer.MAX_VALUE;
                record.setAccessTimeDiff(diff);
            } else if (evictionPolicy == EvictionPolicy.LFU) {
                record.incrementAccessHit();
            }
        }
    }

    protected OffHeapData checkedCastToOffHeapData(Object data) {
        if (!(data instanceof OffHeapData)) {
            throw new IllegalArgumentException("Data must be an instance of "
                    + OffHeapData.class.getName());
        }
        return (OffHeapData) data;
    }

    protected OffHeapData toOffHeapData(Object data) {
        OffHeapData offHeapData = null;
        if (!(data instanceof Data)) {
            offHeapData = serializationService.toData(data, DataType.OFFHEAP);
        } else if (!(data instanceof OffHeapData)) {
            offHeapData = serializationService.convertData((Data)data, DataType.OFFHEAP);
        } else {
            offHeapData = (OffHeapData) data;
        }
        return offHeapData;
    }

    protected boolean isEvictionRequired(MemoryStats memoryStats) {
        return memoryStats.getMaxOffHeap() * evictionThreshold > memoryStats.getFreeOffHeap();
    }

    protected EnterpriseNativeMemoryCacheRecord createRecord(long now) {
        return createRecord(null, now, -1);
    }

    protected EnterpriseNativeMemoryCacheRecord createRecord(Object value, long now) {
        return createRecord(value, now, -1);
    }

    protected EnterpriseNativeMemoryCacheRecord createRecord(long now, long expirationTime) {
        return createRecord(null, now, -1);
    }

    protected EnterpriseNativeMemoryCacheRecord createRecord(Object value, long now, long expirationTime) {
        long address = memoryManager.allocate(EnterpriseNativeMemoryCacheRecord.SIZE);
        EnterpriseNativeMemoryCacheRecord record = cacheRecordService.newRecord();
        record.reset(address);
        record.setCreationTime(now);
        if (expirationTime > 0) {
            record.setExpirationTime(expirationTime);
        }
        if (value != null) {
            OffHeapData offHeapValue = toOffHeapData(value);
            record.setValueAddress(offHeapValue.address());
        } else {
            record.setValueAddress(NULL_PTR);
        }
        return record;
    }

    protected Data putInternal(Data key,
                               Object value,
                               ExpiryPolicy expiryPolicy,
                               boolean getValue,
                               String caller) {
        long now = Clock.currentTimeMillis();
        evictIfRequired(now);

        long creationTime;
        EnterpriseNativeMemoryCacheRecord record = null;
        OffHeapData newValue = null;
        Data oldValue = null;
        boolean newPut = false;

        try {
            record = records.get(key);
            if (record == null) {
                record = createRecord(now);
                creationTime = now;
                records.set(key, record);
                newPut = true;
            } else {
                if (record.getValueAddress() == NULL_PTR) {
                    throw new IllegalStateException("Invalid record -> " + record);
                }
                creationTime = record.getCreationTime();
                if (caller != null) {
                    onEntryInvalidated(key, caller);
                }
            }
            newValue = toOffHeapData(value);
            if (getValue) {
                OffHeapData current = cacheRecordService.readData(record.getValueAddress());
                // TODO: avoid free() until read is completed!
                oldValue = serializationService.convertData(current, DataType.HEAP);
                cacheRecordService.disposeData(current);
            } else {
                cacheRecordService.disposeValue(record);
            }
            record.setValueAddress(newValue.address());

            onAccess(now, record, creationTime);
            if (newPut) {
                record.resetAccessHit();
            }

            long ttlMillis = expiryPolicyToTTL(expiryPolicy);
            ttlMillis = ttlMillis <= 0 ? defaultTTL : ttlMillis;
            ttlMillis = ttlMillis < Integer.MAX_VALUE ? ttlMillis : Integer.MAX_VALUE;
            if (!hasTtl && ttlMillis > 0) {
                hasTtl = true;
            }
            record.setTtlMillis((int) ttlMillis);

            cacheRecordService.enqueueRecord(record);
        } catch (OffHeapOutOfMemoryError e) {
            if (newPut && record != null && record.address() != NULL_PTR) {
                if (!records.delete(key)) {
                    cacheRecordService.dispose(record);
                }
            }
            if (newValue != null && newValue.address() != NULL_PTR) {
                cacheRecordService.disposeData(newValue);
            }
            throw e;
        }
        return oldValue;
    }

    public EnterpriseCacheService getCacheService() {
        return cacheService;
    }

    public CacheRecordAccessor getCacheRecordService() {
        return cacheRecordService;
    }

    public EnterpriseNativeMemoryCacheHashMap getCacheMap() {
        return records;
    }

    public ExpiryPolicy getExpiryPolicy() {
        return expiryPolicy;
    }

    public EvictionPolicy getEvictionPolicy() {
        return evictionPolicy;
    }

    @Override
    public void publishCompletedEvent(String cacheName,
                                      int completionId,
                                      Data dataKey,
                                      int orderKey) {
        if (completionId > 0) {
            cacheService
                    .publishEvent(cacheName,
                            CacheEventType.COMPLETED,
                            dataKey,
                            cacheService.toData(completionId),
                            null,
                            false,
                            orderKey);
        }
    }

    @Override
    public Data get(Data key, ExpiryPolicy expiryPolicy) {
        long now = Clock.currentTimeMillis();
        OffHeapData value = null;
        EnterpriseNativeMemoryCacheRecord record = records.get(key);
        final boolean isExpired = processExpiredEntry(key, record, now);
        if (record != null) {
            long creationTime = record.getCreationTime();
            int ttl = record.getTtlMillis();
            if (ttl <= 0 || creationTime + ttl > now) {
                onAccess(now, record, creationTime);
                long valueAddress = record.getValueAddress();
                value = cacheRecordService.readData(valueAddress);
                cacheRecordService.enqueueRecord(record);
            } else {
                records.delete(key);
            }
        }
        // TODO: avoid free() until read is completed!
        try {
            return serializationService.convertData(value, DataType.HEAP);
        } finally {
            if (value != null) {
                cacheRecordService.enqueueData(value);
            }
        }
    }

    @Override
    public boolean contains(Data key) {
        return records.containsKey(key);
    }

    public void put(Data key, Object value, String caller) {
        put(key, value, expiryPolicy, caller);
    }

    @Override
    public void put(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        putInternal(key, value, expiryPolicy, false, caller);
    }

    @Override
    public Data getAndPut(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        return putInternal(key, value, expiryPolicy, true, caller);
    }

    public void own(Data key, Object value, long ttlMillis) {
        // TODO: Implement without creating a "ExpirePolicy" object
        // This is just a quick workaround
        putInternal(key, value, ttlToExpirePolicy(ttlMillis), true, null);
    }

    @Override
    public CacheRecord getRecord(Data key) {
        return records.get(key);
    }

    @Override
    public void setRecord(Data key, CacheRecord record) {
        if (!(record instanceof EnterpriseNativeMemoryCacheRecord)) {
            throw new IllegalArgumentException("record must be an instance of "
                    + EnterpriseNativeMemoryCacheRecord.class.getName());
        }
        records.set(key, (EnterpriseNativeMemoryCacheRecord)record);
    }

    @Override
    public CacheRecord removeRecord(Data key) {
        return records.remove(key);
    }

    @Override
    public Map<Data, CacheRecord> getReadOnlyRecords() {
        return Collections.unmodifiableMap((Map<Data, CacheRecord>) (Map) records);
    }

    @Override
    public boolean putIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        long now = Clock.currentTimeMillis();
        evictIfRequired(now);
        EnterpriseNativeMemoryCacheRecord record = null;
        OffHeapData newValue = null;

        try {
            record = records.get(key);
            if (record != null) {
                long creationTime = record.getCreationTime();
                int ttl = record.getTtlMillis();
                if (ttl <= 0 || creationTime + ttl > now) {
                    return false;
                }
                cacheRecordService.disposeValue(record);
            } else {
                record = createRecord(now);
                records.set(key, record);
            }

            record.setCreationTime(now);
            record.setAccessTimeDiff(0);
            record.setTtlMillis(-1);

            newValue = toOffHeapData(value);

            record.setValueAddress(newValue.address());

            cacheRecordService.enqueueRecord(record);
        } catch (OffHeapOutOfMemoryError e) {
            if (record != null && record.address() != NULL_PTR) {
                if (!records.delete(key)) {
                    cacheRecordService.dispose(record);
                }
            }
            if (newValue != null && newValue.address() != NULL_PTR) {
                cacheRecordService.disposeData(newValue);
            }
            throw e;
        }
        return true;
    }

    public boolean replace(Data key, Object value, String caller) {
        return replace(key, value, expiryPolicy, caller);
    }

    @Override
    public boolean replace(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        long now = Clock.currentTimeMillis();
        evictIfRequired(now);
        EnterpriseNativeMemoryCacheRecord record = records.get(key);
        if (record != null) {
            onEntryInvalidated(key, caller);
            long creationTime = record.getCreationTime();
            int ttl = record.getTtlMillis();
            if (ttl <= 0 || creationTime + ttl > now) {
                OffHeapData newValue = toOffHeapData(value);
                cacheRecordService.disposeValue(record);
                record.setValueAddress(newValue.address());

                onAccess(now, record, creationTime);

                cacheRecordService.enqueueRecord(record);
                return true;
            } else {
                cacheRecordService.dispose(record);
            }
        }
        return false;
    }

    public boolean replace(Data key, Object oldValue, Object newValue, String caller) {
        return replace(key, oldValue, newValue, expiryPolicy, caller);
    }

    @Override
    public boolean replace(Data key,
                           Object oldValue,
                           Object newValue,
                           ExpiryPolicy expiryPolicy,
                           String caller) {
        long now = Clock.currentTimeMillis();
        evictIfRequired(now);
        EnterpriseNativeMemoryCacheRecord record = records.get(key);
        if (record != null) {
            long creationTime = record.getCreationTime();
            int ttl = record.getTtlMillis();
            if (ttl <= 0 || creationTime + ttl > now) {
                Object existingValue = getRecordValue(record);
                if (oldValue.equals(existingValue)) {
                    onEntryInvalidated(key, caller);
                    OffHeapData newOffHeapData = toOffHeapData(newValue);
                    cacheRecordService.disposeValue(record);
                    record.setValueAddress(newOffHeapData.address());

                    onAccess(now, record, creationTime);
                    return true;
                }
                cacheRecordService.enqueueRecord(record);
            } else {
                onEntryInvalidated(key, caller);
                cacheRecordService.dispose(record);
            }
        }
        return false;
    }

    @Override
    public Object getAndReplace(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        long now = Clock.currentTimeMillis();
        evictIfRequired(now);

        Data oldValue = null;
        EnterpriseNativeMemoryCacheRecord record = records.get(key);
        if (record != null) {
            onEntryInvalidated(key, caller);
            long creationTime = record.getCreationTime();
            int ttl = record.getTtlMillis();
            if (ttl <= 0 || creationTime + ttl > now) {
                OffHeapData newValue = toOffHeapData(value);
                OffHeapData oldBinary = cacheRecordService.readData(record.getValueAddress());
                oldValue = serializationService.convertData(oldBinary, DataType.HEAP);
                cacheRecordService.disposeData(oldBinary);
                record.setValueAddress(newValue.address());

                onAccess(now, record, creationTime);

                cacheRecordService.enqueueRecord(record);
            } else {
                cacheRecordService.dispose(record);
            }
        }
        return oldValue;
    }

    public int evictIfRequired(long now) {
        if (evictionEnabled) {
            MemoryStats memoryStats = memoryManager.getMemoryStats();
            if (isEvictionRequired(memoryStats)) {
                return records.evictRecords(evictionPercentage, evictionPolicy);
            }
        }
        return 0;
    }

    @Override
    public int forceEvict() {
        int percentage = Math.max(MIN_FORCED_EVICT_PERCENTAGE, evictionPercentage);
        int evicted = 0;
        if (hasTTL()) {
            evicted = records.evictExpiredRecords(100);
        }
        evicted += records.evictRecords(percentage, EvictionPolicy.RANDOM);
        return evicted;
    }

    public void evictExpiredRecords(int percentage) {
        records.evictExpiredRecords(percentage);
    }

    @Override
    public Data getAndRemove(Data key, String caller) {
        Data oldValue = null;
        EnterpriseNativeMemoryCacheRecord record = records.remove(key);
        if (record != null) {
            onEntryInvalidated(key, caller);
            OffHeapData oldBinary = cacheRecordService.readData(record.getValueAddress());
            oldValue = serializationService.convertData(oldBinary, DataType.HEAP);
            cacheRecordService.dispose(record);
        }
        return oldValue;
    }

    @Override
    public boolean remove(Data key, String caller) {
        boolean deleted = records.delete(key);
        if (deleted) {
            onEntryInvalidated(key, caller);
        }
        return deleted;
    }

    @Override
    public boolean remove(Data key, Object value, String caller) {
        boolean deleted = false;
        EnterpriseNativeMemoryCacheRecord record = records.get(key);
        if (record != null) {
            Object existingValue = getRecordValue(record);
            if (value.equals(existingValue)) {
                onEntryInvalidated(key, caller);
                records.delete(key);
                deleted = true;
            }
            cacheRecordService.enqueueRecord(record);
        }
        return deleted;
    }

    @Override
    public void removeAll(Set<Data> keys) {
        final long now = Clock.currentTimeMillis();
        final Set<Data> localKeys = new HashSet<Data>(keys.isEmpty() ? records.keySet() : keys);
        try {
            deleteAllCacheEntry(localKeys);
        } finally {
            final Set<Data> keysToClean = new HashSet<Data>(keys.isEmpty() ? records.keySet() : keys);
            for (Data key : keysToClean) {
                isEventBatchingEnabled = true;
                final EnterpriseNativeMemoryCacheRecord record = records.get(key);
                if (localKeys.contains(key) && record != null) {
                    final boolean isExpired = processExpiredEntry(key, record, now);
                    if (!isExpired) {
                        records.remove(key);
                        if (isStatisticsEnabled()) {
                            statistics.increaseCacheRemovals(1);
                        }
                    }
                } else {
                    keys.remove(key);
                }
                isEventBatchingEnabled = false;
                int orderKey = keys.hashCode();
                publishBatchedEvents(cacheConfig.getName(), CacheEventType.REMOVED, orderKey);
            }
        }
    }

    @Override
    public MapEntrySet getAll(Set<Data> keySet, ExpiryPolicy expiryPolicy) {
        final ExpiryPolicy localExpiryPolicy =
                expiryPolicy != null
                        ? expiryPolicy
                        : this.expiryPolicy;
        final MapEntrySet result = new MapEntrySet();
        for (Data key : keySet) {
            final Object value = get(key, localExpiryPolicy);
            if (value != null) {
                result.add(key, cacheService.toData(value));
            }
        }
        return result;
    }

    @Override
    public Set<Data> loadAll(Set<Data> keys, boolean replaceExistingValues) {
        Set<Data> keysLoaded = new HashSet<Data>();
        Map<Data, Object> loaded = loadAllCacheEntry(keys);
        if (loaded == null || loaded.isEmpty()) {
            return keysLoaded;
        }
        if (replaceExistingValues) {
            for (Map.Entry<Data, Object> entry : loaded.entrySet()) {
                final Data key = entry.getKey();
                final Object value = entry.getValue();
                if (value != null) {
                    getAndPut(key, value, expiryPolicy, null);
                    keysLoaded.add(key);
                }
            }
        } else {
            for (Map.Entry<Data, Object> entry : loaded.entrySet()) {
                final Data key = entry.getKey();
                final Object value = entry.getValue();
                if (value != null) {
                    final boolean hasPut = putIfAbsent(key, value, expiryPolicy, null);
                    if (hasPut) {
                        keysLoaded.add(key);
                    }
                }
            }
        }
        return keysLoaded;
    }

    @Override
    public Object invoke(Data key, EntryProcessor entryProcessor, Object[] arguments) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        EnterpriseNativeMemoryCacheRecord record = records.get(key);
        final boolean isExpired = processExpiredEntry(key, record, now);
        if (isExpired) {
            record = null;
        }

        if (isStatisticsEnabled()) {
            if (record == null || isExpired) {
                statistics.increaseCacheMisses(1);
            } else {
                statistics.increaseCacheHits(1);
            }
        }
        if (isStatisticsEnabled()) {
            statistics.addGetTimeNano(System.nanoTime() - start);
        }

        EnterpriseNativeMemoryCacheEntryProcessorEntry entry =
                new EnterpriseNativeMemoryCacheEntryProcessorEntry(key, record, this, now);
        final Object process = entryProcessor.process(entry, arguments);
        entry.applyChanges();
        return process;
    }

    @Override
    public CacheKeyIteratorResult iterator(int tableIndex, int size) {
        return records.fetchNext(tableIndex, size);
    }

    public final boolean hasTTL() {
        return hasTtl;
    }

    @Override
    public final int size() {
        return records.size();
    }

    @Override
    public final void clear() {
        onClear();
        records.clear();
    }

    @Override
    public final void destroy() {
        clear();
        onDestroy();
        records.destroy();
    }

    @Override
    public CacheConfig getConfig() {
        return cacheConfig;
    }

    @Override
    public String getName() {
        return cacheConfig.getName();
    }

    @Override
    public CacheStatisticsImpl getCacheStats() {
        return statistics;
    }

    public static class CacheRecordAccessor
            implements MemoryBlockAccessor<EnterpriseNativeMemoryCacheRecord> {

        private final EnterpriseSerializationService ss;
        private final MemoryManager memoryManager;
        private final Queue<EnterpriseNativeMemoryCacheRecord> recordQ = new ArrayDeque<EnterpriseNativeMemoryCacheRecord>(1024);
        private final Queue<OffHeapData> dataQ = new ArrayDeque<OffHeapData>(1024);

        public CacheRecordAccessor(EnterpriseSerializationService ss) {
            this.ss = ss;
            this.memoryManager = ss.getMemoryManager();
        }

        @Override
        public boolean isEqual(long address, EnterpriseNativeMemoryCacheRecord value) {
            return isEqual(address, value.address());
        }

        @Override
        public boolean isEqual(long address1, long address2) {
            long valueAddress1 = UnsafeHelper.UNSAFE.getLong(address1 + EnterpriseNativeMemoryCacheRecord.VALUE_OFFSET);
            long valueAddress2 = UnsafeHelper.UNSAFE.getLong(address2 + EnterpriseNativeMemoryCacheRecord.VALUE_OFFSET);
            return OffHeapDataUtil.equals(valueAddress1, valueAddress2);
        }

        public EnterpriseNativeMemoryCacheRecord newRecord() {
            EnterpriseNativeMemoryCacheRecord record = recordQ.poll();
            if (record == null) {
                record = new EnterpriseNativeMemoryCacheRecord();
            }
            return record;
        }

        @Override
        public EnterpriseNativeMemoryCacheRecord read(long address) {
            if (address <= NULL_PTR) {
                throw new IllegalArgumentException("Illegal memory address: " + address);
            }
            EnterpriseNativeMemoryCacheRecord record = newRecord();
            record.reset(address);
            return record;
        }

        @Override
        public void dispose(EnterpriseNativeMemoryCacheRecord record) {
            if (record.address() <= NULL_PTR) {
                throw new IllegalArgumentException("Illegal memory address: " + record.address());
            }
            disposeValue(record);
            record.clear();
            memoryManager.free(record.address(), record.size());
            recordQ.offer(record.reset(NULL_PTR));
        }

        @Override
        public void dispose(long address) {
            dispose(read(address));
        }

        public OffHeapData readData(long valueAddress) {
            if (valueAddress <= NULL_PTR) {
                throw new IllegalArgumentException("Illegal memory address: " + valueAddress);
            }
            OffHeapData value = dataQ.poll();
            if (value == null) {
                value = new OffHeapData();
            }
            return value.reset(valueAddress);
        }

        public void disposeValue(EnterpriseNativeMemoryCacheRecord record) {
            long valueAddress = record.getValueAddress();
            if (valueAddress != NULL_PTR) {
                disposeData(valueAddress);
                record.setValueAddress(NULL_PTR);
            }
        }

        public void disposeData(Data value) {
            if (value instanceof OffHeapData) {
                ss.disposeData(value);
                dataQ.offer((OffHeapData) value);
            }
        }

        void disposeData(long address) {
            OffHeapData data = readData(address);
            disposeData(data);
        }

        void enqueueRecord(EnterpriseNativeMemoryCacheRecord record) {
            recordQ.offer(record.reset(NULL_PTR));
        }

        void enqueueData(OffHeapData data) {
            data.reset(NULL_PTR);
            dataQ.offer(data);
        }
    }

    protected Callback<Data> createEvictionCallback() {
        return new Callback<Data>() {
            public void notify(Data object) {
                cacheService.sendInvalidationEvent(cacheConfig.getName(), object, "<NA>");
            }
        };
    }

    protected void onEntryInvalidated(Data key, String source) {
        cacheService.sendInvalidationEvent(cacheConfig.getName(), key, source);
    }

    protected void onClear() {
        cacheService.sendInvalidationEvent(cacheConfig.getName(), null, "<NA>");
    }

    protected void onDestroy() {
        cacheService.sendInvalidationEvent(cacheConfig.getName(), null, "<NA>");
        ScheduledFuture<?> f = evictionTaskFuture;
        if (f != null) {
            f.cancel(true);
        }
    }

    public BinaryOffHeapHashMap<EnterpriseNativeMemoryCacheRecord>.EntryIter iterator(int slot) {
        return records.iterator(slot);
    }

    protected class EvictionTask implements Runnable {
        public void run() {
            if (hasTTL()) {
                OperationService operationService = nodeEngine.getOperationService();
                operationService.executeOperation(evictionOperation);
            }
        }
    }

    protected Operation createEvictionOperation(int percentage) {
        return new CacheEvictionOperation(cacheConfig.getName(), percentage)
                .setNodeEngine(nodeEngine)
                .setPartitionId(partitionId)
                .setCallerUuid(nodeEngine.getLocalMember().getUuid())
                .setService(cacheService);
    }

}
