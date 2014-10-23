package com.hazelcast.cache.enterprise.impl.hidensity.nativememory;

import com.hazelcast.cache.enterprise.*;
import com.hazelcast.cache.enterprise.operation.CacheEvictionOperation;
import com.hazelcast.cache.impl.*;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.elasticcollections.map.BinaryOffHeapHashMap;
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

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author sozal 14/10/14
 */
public class HiDensityNativeMemoryCacheRecordStore
        extends AbstractCacheRecordStore<
                    HiDensityNativeMemoryCacheRecord,
                    HiDensityNativeMemoryCacheRecordMap> {

    public static final int DEFAULT_INITIAL_CAPACITY = 1000;
    public static final long NULL_PTR = MemoryManager.NULL_ADDRESS;

    private final int initialCapacity;
    private final EnterpriseSerializationService serializationService;
    private final ScheduledFuture<?> evictionTaskFuture;
    private final Operation evictionOperation;
    private MemoryManager memoryManager;
    private CacheRecordAccessor cacheRecordAccessor;
    private final EvictionPolicy evictionPolicy;
    private final boolean evictionEnabled;
    private final int evictionPercentage;
    private final float evictionThreshold;
    private volatile boolean hasTtl;
    private final long defaultTTL;

    protected HiDensityNativeMemoryCacheRecordStore(final int partitionId,
                                                    final String name,
                                                    final EnterpriseCacheService cacheService,
                                                    final NodeEngine nodeEngine,
                                                    final int initialCapacity,
                                                    final ExpiryPolicy expiryPolicy,
                                                    final EvictionPolicy evictionPolicy,
                                                    final int evictionPercentage,
                                                    final int evictionThresholdPercentage) {
        super(name, partitionId, nodeEngine, cacheService, expiryPolicy);
        this.initialCapacity = initialCapacity;
        this.serializationService = (EnterpriseSerializationService)nodeEngine.getSerializationService();
        this.evictionPolicy = evictionPolicy != null ? evictionPolicy : cacheConfig.getEvictionPolicy();
        this.cacheRecordAccessor = new CacheRecordAccessor(serializationService);
        this.memoryManager = serializationService.getMemoryManager();
        this.records = createRecordCacheMap();
        this.evictionEnabled = evictionPolicy != EvictionPolicy.NONE;
        this.evictionPercentage = evictionPercentage;
        this.evictionThreshold = (float) Math.max(1, 100 - evictionThresholdPercentage) / 100;
        long ttl = expiryPolicyToTTL(defaultExpiryPolicy);
        this.defaultTTL = ttl > 0 ? ttl : DEFAULT_TTL;
        this.hasTtl = defaultTTL > 0;
        this.evictionOperation = createEvictionOperation(10);
        this.evictionTaskFuture =
                nodeEngine.getExecutionService()
                    .scheduleWithFixedDelay("hz:cache", new EvictionTask(), 5, 5, TimeUnit.SECONDS);
    }

    public HiDensityNativeMemoryCacheRecordStore(final int partitionId,
                                                 final String cacheName,
                                                 final EnterpriseCacheService cacheService,
                                                 final NodeEngine nodeEngine,
                                                 final int initialCapacity) {
        this(partitionId,
             cacheName,
             cacheService,
             nodeEngine,
             initialCapacity,
             null,
             null,
             DEFAULT_EVICTION_PERCENTAGE,
             DEFAULT_EVICTION_THRESHOLD_PERCENTAGE);
    }

    @Override
    protected HiDensityNativeMemoryCacheRecordMap createRecordCacheMap() {
        if (records != null) {
            return records;
        }
        if (serializationService == null || cacheRecordAccessor == null) {
            return null;
        }
        return
            new HiDensityNativeMemoryCacheRecordMap(initialCapacity,
                                                    serializationService,
                                                    cacheRecordAccessor,
                                                    createEvictionCallback());
    }

    @Override
    protected HiDensityNativeMemoryCacheRecord createRecord(Object value,
                                                            long creationTime,
                                                            long expiryTime) {
        OffHeapData offHeapValue = null;
        HiDensityNativeMemoryCacheRecord record = null;
        try {
            long address = memoryManager.allocate(HiDensityNativeMemoryCacheRecord.SIZE);
            record = cacheRecordAccessor.newRecord();
            record.reset(address);
            if (creationTime > 0) {
                record.setCreationTime(creationTime);
            }
            if (expiryTime > 0) {
                record.setExpirationTime(expiryTime);
            }
            if (value != null) {
                offHeapValue = toOffHeapData(value);
                record.setValueAddress(offHeapValue.address());
            } else {
                record.setValueAddress(HiDensityNativeMemoryCacheRecordStore.NULL_PTR);
            }
            return record;
        } catch (OffHeapOutOfMemoryError e) {
            if (record != null && record.address() >= NULL_PTR) {
                cacheRecordAccessor.dispose(record);
            }
            if (offHeapValue != null && offHeapValue.address() >= NULL_PTR) {
                cacheRecordAccessor.disposeData(offHeapValue);
            }
            throw e;
        }
    }

    @Override
    protected <T> Data valueToData(T value) {
        return serializationService.toData(value, DataType.OFFHEAP);
    }

    @Override
    protected <T> T dataToValue(Data data) {
        return (T) cacheService.toObject(data);
    }

    @Override
    protected <T> HiDensityNativeMemoryCacheRecord valueToRecord(T value) {
        return createRecord(value, -1, -1);
    }

    @Override
    protected <T> T recordToValue(HiDensityNativeMemoryCacheRecord record) {
        return (T) cacheRecordAccessor.readValue(record, true);
    }

    @Override
    protected Data recordToData(HiDensityNativeMemoryCacheRecord record) {
        Object value = recordToValue(record);
        if (value == null) {
            return null;
        } else if (value instanceof Data) {
            return (Data) value;
        } else {
            return valueToData(value);
        }
    }

    @Override
    protected HiDensityNativeMemoryCacheRecord dataToRecord(Data data) {
        Object value = dataToValue(data);
        if (value == null) {
            return null;
        } else if (value instanceof HiDensityNativeMemoryCacheRecord) {
            return (HiDensityNativeMemoryCacheRecord) value;
        } else {
            return valueToRecord(value);
        }
    }

    private OffHeapData getRecordData(HiDensityNativeMemoryCacheRecord record) {
        return cacheRecordAccessor.readData(record.getValueAddress());
    }

    private OffHeapData toOffHeapData(Object data) {
        OffHeapData offHeapData = null;
        if (!(data instanceof Data)) {
            offHeapData = serializationService.toData(data, DataType.OFFHEAP);
        } else if (!(data instanceof OffHeapData)) {
            offHeapData = serializationService.convertData((Data) data, DataType.OFFHEAP);
        } else {
            offHeapData = (OffHeapData) data;
        }
        return offHeapData;
    }

    Object getDataValue(Data data) {
        if (data != null) {
            return serializationService.toObject(data);// serializationService.convertData(data, DataType.HEAP);
        } else {
            return null;
        }
    }

    Object getRecordValue(HiDensityNativeMemoryCacheRecord record) {
        return getDataValue(getRecordData(record));
    }

    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    public EnterpriseCacheService getCacheService() {
        return (EnterpriseCacheService) cacheService;
    }

    public CacheRecordAccessor getCacheRecordAccessor() {
        return cacheRecordAccessor;
    }

    protected void deleteRecord(Data key) {
        final HiDensityNativeMemoryCacheRecord record = records.remove(key);
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

    HiDensityNativeMemoryCacheRecord accessRecord(HiDensityNativeMemoryCacheRecord record,
                                                  ExpiryPolicy expiryPolicy,
                                                  long now) {
        updateAccessDuration(record, getExpiryPolicy(expiryPolicy), now);
        return record;
    }

    /*
    HiDensityNativeMemoryCacheRecord readThroughRecord(Data key, long now) {
        Object value = readThroughCache(key);
        if (value == null) {
            return null;
        }
        Duration expiryDuration;
        try {
            expiryDuration = defaultExpiryPolicy.getExpiryForCreation();
        } catch (Exception e) {
            expiryDuration = Duration.ETERNAL;
        }
        long expiryTime = expiryDuration.getAdjustedTime(now);

        if (isExpiredAt(expiryTime, now)) {
            return null;
        }
        return createRecord(key, value, expiryTime);
    }
    */

    /*
    void deleteThroughCache(Data key) {
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
    */

    /*
    void deleteAllCacheEntry(Set<Data> keys) {
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
    */

    /*
    Map<Data, Object> loadAllCacheEntry(Set<Data> keys) {
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
    */

    void onAccess(long now,
                  HiDensityNativeMemoryCacheRecord record,
                  long creationTime) {
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

    private boolean isEvictionRequired(MemoryStats memoryStats) {
        return memoryStats.getMaxOffHeap() * evictionThreshold > memoryStats.getFreeOffHeap();
    }

    private Data putInternal(Data key,
                             Object value,
                             ExpiryPolicy expiryPolicy,
                             boolean getValue,
                             String caller) {
        long now = Clock.currentTimeMillis();
        evictIfRequired(now);

        long creationTime;
        HiDensityNativeMemoryCacheRecord record = null;
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
            if (getValue && !newPut) {
                OffHeapData current = cacheRecordAccessor.readData(record.getValueAddress());
                // TODO: avoid free() until read is completed!
                oldValue = serializationService.convertData(current, DataType.HEAP);
                cacheRecordAccessor.disposeData(current);
            } else {
                cacheRecordAccessor.disposeValue(record);
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

            cacheRecordAccessor.enqueueRecord(record);
        } catch (OffHeapOutOfMemoryError e) {
            if (newPut && record != null && record.address() != NULL_PTR) {
                if (!records.delete(key)) {
                    cacheRecordAccessor.dispose(record);
                }
            }
            if (newValue != null && newValue.address() != NULL_PTR) {
                cacheRecordAccessor.disposeData(newValue);
            }
            throw e;
        }
        return oldValue;
    }

    /*
    @Override
    public Object get(Data key, ExpiryPolicy expiryPolicy) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        long now = Clock.currentTimeMillis();
        Object value = null;
        HiDensityNativeMemoryCacheRecord record = records.get(key);
        final boolean isExpired = processExpiredEntry(key, record, now);
        if (record == null || isExpired) {
            if (isStatisticsEnabled()) {
                statistics.increaseCacheMisses(1);
            }
            value = readThroughCache(key);
            if (value == null) {
                return null;
            }
            createRecordWithExpiry(key, value, expiryPolicy, now, true);
            return value;
        }
        else {
            value = recordToValue(record);
            updateAccessDuration(record, expiryPolicy, now);
            if (isStatisticsEnabled()) {
                statistics.increaseCacheHits(1);
            }
            cacheRecordAccessor.enqueueRecord(record);
            return value;
        }
    }
    */

    protected void onGet(Data key,
                         ExpiryPolicy expiryPolicy,
                         Object value,
                         HiDensityNativeMemoryCacheRecord record) {
        cacheRecordAccessor.enqueueRecord(record);
    }

    /*
    public Data get(Data key, ExpiryPolicy expiryPolicy) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        long now = Clock.currentTimeMillis();
        OffHeapData value = null;
        HiDensityNativeMemoryCacheRecord record = records.get(key);
        final boolean isExpired = processExpiredEntry(key, record, now);
        if (record != null) {
            long creationTime = record.getCreationTime();
            int ttl = record.getTtlMillis();
            if (ttl <= 0 || creationTime + ttl > now) {
                onAccess(now, record, creationTime);
                long valueAddress = record.getValueAddress();
                value = cacheRecordAccessor.readData(valueAddress);
                cacheRecordAccessor.enqueueRecord(record);
            } else {
                records.delete(key);
            }
        }
        // TODO: avoid free() until read is completed!
        try {
            return serializationService.convertData(value, DataType.HEAP);
        } finally {
            if (value != null) {
                cacheRecordAccessor.enqueueData(value);
            }
        }
    }
    */

    /*
    public Object get(Data key, ExpiryPolicy expiryPolicy) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        long now = Clock.currentTimeMillis();
        Object value;
        CacheRecord record = records.get(key);
        final boolean isExpired = processExpiredEntry(key, record, now);
        if (record == null || isExpired) {
            if (isStatisticsEnabled()) {
                statistics.increaseCacheMisses(1);
            }
            value = readThroughCache(key);
            if (value == null) {
                return null;
            }
            createRecordWithExpiry(key, value, expiryPolicy, now, true);

            return value;
        } else {
            value = record.getValue();
            updateAccessDuration(record, expiryPolicy, now);
            if (isStatisticsEnabled()) {
                statistics.increaseCacheHits(1);
            }
            return value;
        }
    }
     */

    /*
    @Override
    public boolean contains(Data key) {
        return records.containsKey(key);
    }
    */

    /*
    protected Object getAndPut(Data key,
                               Object value,
                               ExpiryPolicy expiryPolicy,
                               String caller,
                               boolean getValue,
                               boolean disableWriteThrough) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        boolean isPutSucceed;
        Object oldValue = null;
        R record = records.get(key);
        boolean isExpired = processExpiredEntry(key, record, now);
        // check that new entry is not already expired, in which case it should
        // not be added to the cache or listeners called or writers called.
        if (record == null || isExpired) {
            isPutSucceed = createRecordWithExpiry(key,
                                                  value,
                                                  expiryPolicy,
                                                  now,
                                                  disableWriteThrough);
        } else {
            oldValue = record.getValue();
            isPutSucceed = updateRecordWithExpiry(key,
                                                  value,
                                                  record,
                                                  expiryPolicy,
                                                  now,
                                                  disableWriteThrough);
        }
        updateGetAndPutStat(isPutSucceed, getValue, oldValue == null, start);
        return oldValue;
    }
    */

    public void put(Data key, Object value, String caller) {
        put(key, value, defaultExpiryPolicy, caller);
    }

    @Override
    public void put(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        putInternal(key, value, expiryPolicy, false, caller); //getAndPut(key, value, expiryPolicy, caller, false, false);
    }

    @Override
    public Object getAndPut(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        return putInternal(key, value, expiryPolicy, true, caller); //getAndPut(key, value, expiryPolicy, caller, true, false);
    }

    protected void onBeforeGetAndPut(Data key,
                                     Object value,
                                     ExpiryPolicy expiryPolicy,
                                     String caller,
                                     boolean getValue,
                                     boolean disableWriteThrough,
                                     HiDensityNativeMemoryCacheRecord record,
                                     Object oldValue,
                                     boolean isExpired,
                                     boolean willBeNewPut) {
        /*
        if (!willBeNewPut) {
            if (record == null || record.getValueAddress() == NULL_PTR) {
                throw new IllegalStateException("Invalid record -> " + record);
            }
        }
        if (record != null && record.getValueAddress() != NULL_PTR) {
            if (getValue) {
                OffHeapData current = cacheRecordAccessor.readData(record.getValueAddress());
                cacheRecordAccessor.disposeData(current);
            } else {
                cacheRecordAccessor.disposeValue(record);
            }
        }
        */
    }

    protected void onAfterGetAndPut(Data key,
                                    Object value,
                                    ExpiryPolicy expiryPolicy,
                                    String caller,
                                    boolean getValue,
                                    boolean disableWriteThrough,
                                    HiDensityNativeMemoryCacheRecord record,
                                    boolean isExpired,
                                    boolean isNewPut,
                                    boolean isSaveSucceed) {
        /*
        long ttlMillis = expiryPolicyToTTL(expiryPolicy);
        ttlMillis = ttlMillis <= 0 ? defaultTTL : ttlMillis;
        ttlMillis = ttlMillis < Integer.MAX_VALUE ? ttlMillis : Integer.MAX_VALUE;
        if (!hasTtl && ttlMillis > 0) {
            hasTtl = true;
        }
        cacheRecordAccessor.enqueueRecord(record);
        */
    }

    protected void onGetAndPutError(Data key,
                                    Object value,
                                    ExpiryPolicy expiryPolicy,
                                    String caller,
                                    boolean getValue,
                                    boolean disableWriteThrough,
                                    HiDensityNativeMemoryCacheRecord record,
                                    boolean wouldBeNewPut,
                                    Throwable error) {
        /*
        if (wouldBeNewPut && record != null && record.address() != NULL_PTR) {
            if (!records.delete(key)) {
                cacheRecordAccessor.dispose(record);
            }
        }
        */
    }

    public void own(Data key, Object value, long ttlMillis) {
        // TODO: Implement without creating a "ExpirePolicy" object
        // This is just a quick workaround
        putInternal(key, value, ttlToExpirePolicy(ttlMillis), false, null);
    }

    @Override
    public CacheRecord getRecord(Data key) {
        return records.get(key);
    }

    @Override
    public void setRecord(Data key, CacheRecord record) {
        if (!(record instanceof HiDensityNativeMemoryCacheRecord)) {
            throw new IllegalArgumentException("record must be an instance of "
                    + HiDensityNativeMemoryCacheRecord.class.getName());
        }
        records.set(key, (HiDensityNativeMemoryCacheRecord)record);
    }

    @Override
    public CacheRecord removeRecord(Data key) {
        return records.remove(key);
    }

    public boolean putIfAbsent(Data key, Object value, String caller) {
        return putIfAbsent(key, value, defaultExpiryPolicy, caller);
    }

    @Override
    public boolean putIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        long now = Clock.currentTimeMillis();
        evictIfRequired(now);
        HiDensityNativeMemoryCacheRecord record = null;
        OffHeapData newValue = null;

        try {
            record = records.get(key);
            if (record != null) {
                long creationTime = record.getCreationTime();
                int ttl = record.getTtlMillis();
                if (ttl <= 0 || creationTime + ttl > now) {
                    return false;
                }
                cacheRecordAccessor.disposeValue(record);
            } else {
                record = createRecord(now);
                records.set(key, record);
            }

            record.setCreationTime(now);
            record.setAccessTimeDiff(0);
            record.setTtlMillis(-1);

            newValue = toOffHeapData(value);

            record.setValueAddress(newValue.address());

            cacheRecordAccessor.enqueueRecord(record);
        } catch (OffHeapOutOfMemoryError e) {
            if (record != null && record.address() != NULL_PTR) {
                if (!records.delete(key)) {
                    cacheRecordAccessor.dispose(record);
                }
            }
            if (newValue != null && newValue.address() != NULL_PTR) {
                cacheRecordAccessor.disposeData(newValue);
            }
            throw e;
        }
        return true;
    }

    public boolean replace(Data key, Object value, String caller) {
        return replace(key, value, defaultExpiryPolicy, caller);
    }

    @Override
    public boolean replace(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        long now = Clock.currentTimeMillis();
        evictIfRequired(now);
        HiDensityNativeMemoryCacheRecord record = records.get(key);
        if (record != null) {
            onEntryInvalidated(key, caller);
            long creationTime = record.getCreationTime();
            int ttl = record.getTtlMillis();
            if (ttl <= 0 || creationTime + ttl > now) {
                OffHeapData newValue = toOffHeapData(value);
                cacheRecordAccessor.disposeValue(record);
                record.setValueAddress(newValue.address());

                onAccess(now, record, creationTime);

                cacheRecordAccessor.enqueueRecord(record);
                return true;
            } else {
                cacheRecordAccessor.dispose(record);
            }
        }
        return false;
    }

    public boolean replace(Data key, Object oldValue, Object newValue, String caller) {
        return replace(key, oldValue, newValue, defaultExpiryPolicy, caller);
    }

    @Override
    public boolean replace(Data key,
                           Object oldValue,
                           Object newValue,
                           ExpiryPolicy expiryPolicy,
                           String caller) {
        long now = Clock.currentTimeMillis();
        evictIfRequired(now);
        HiDensityNativeMemoryCacheRecord record = records.get(key);
        if (record != null) {
            long creationTime = record.getCreationTime();
            int ttl = record.getTtlMillis();
            if (ttl <= 0 || creationTime + ttl > now) {
                Object existingValue = toValue(record);
                if (toValue(oldValue).equals(existingValue)) {
                    onEntryInvalidated(key, caller);
                    OffHeapData newOffHeapData = toOffHeapData(newValue);
                    cacheRecordAccessor.disposeValue(record);
                    record.setValueAddress(newOffHeapData.address());

                    onAccess(now, record, creationTime);
                    return true;
                }
                cacheRecordAccessor.enqueueRecord(record);
            } else {
                onEntryInvalidated(key, caller);
                cacheRecordAccessor.dispose(record);
            }
        }
        return false;
    }

    public Object getAndReplace(Data key, Object value, String caller) {
        return getAndReplace(key, value, defaultExpiryPolicy, caller);
    }

    @Override
    public Object getAndReplace(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        long now = Clock.currentTimeMillis();
        evictIfRequired(now);

        Data oldValue = null;
        HiDensityNativeMemoryCacheRecord record = records.get(key);
        if (record != null) {
            onEntryInvalidated(key, caller);
            long creationTime = record.getCreationTime();
            int ttl = record.getTtlMillis();
            if (ttl <= 0 || creationTime + ttl > now) {
                OffHeapData newValue = toOffHeapData(value);
                OffHeapData oldBinary = cacheRecordAccessor.readData(record.getValueAddress());
                oldValue = serializationService.convertData(oldBinary, DataType.HEAP);
                cacheRecordAccessor.disposeData(oldBinary);
                record.setValueAddress(newValue.address());

                onAccess(now, record, creationTime);

                cacheRecordAccessor.enqueueRecord(record);
            } else {
                cacheRecordAccessor.dispose(record);
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
        HiDensityNativeMemoryCacheRecord record = records.remove(key);
        if (record != null) {
            onEntryInvalidated(key, caller);
            OffHeapData oldBinary = cacheRecordAccessor.readData(record.getValueAddress());
            oldValue = serializationService.convertData(oldBinary, DataType.HEAP);
            cacheRecordAccessor.dispose(record);
        }
        return oldValue;
    }

    /*
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
        HiDensityNativeMemoryCacheRecord record = records.get(key);
        if (record != null) {
            Object existingValue = toValue(record);
            if (toValue(value).equals(existingValue)) {
                onEntryInvalidated(key, caller);
                records.delete(key);
                deleted = true;
            }
            cacheRecordAccessor.enqueueRecord(record);
        }
        return deleted;
    }
    */

    protected void onRemove(Data key,
                            Object value,
                            HiDensityNativeMemoryCacheRecord record,
                            boolean removed) {
        cacheRecordAccessor.enqueueRecord(record);
    }

    /*
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
                final HiDensityNativeMemoryCacheRecord record = records.get(key);
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
    */

    /*
    @Override
    public MapEntrySet getAll(Set<Data> keySet, ExpiryPolicy expiryPolicy) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        final MapEntrySet result = new MapEntrySet();
        for (Data key : keySet) {
            final Object value = get(key, expiryPolicy);
            if (value != null) {
                result.add(key, cacheService.toData(value));
            }
        }
        return result;
    }
    */

    /*
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
                    getAndPut(key, value, defaultExpiryPolicy, null);
                    keysLoaded.add(key);
                }
            }
        } else {
            for (Map.Entry<Data, Object> entry : loaded.entrySet()) {
                final Data key = entry.getKey();
                final Object value = entry.getValue();
                if (value != null) {
                    final boolean hasPut = putIfAbsent(key, value, defaultExpiryPolicy, null);
                    if (hasPut) {
                        keysLoaded.add(key);
                    }
                }
            }
        }
        return keysLoaded;
    }
    */

    @Override
    public Object invoke(Data key, EntryProcessor entryProcessor, Object[] arguments) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        HiDensityNativeMemoryCacheRecord record = records.get(key);
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

        HiDensityNativeMemoryCacheEntryProcessorEntry entry =
                new HiDensityNativeMemoryCacheEntryProcessorEntry(key, record, this, now);
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

    public static class CacheRecordAccessor
            implements MemoryBlockAccessor<HiDensityNativeMemoryCacheRecord> {

        private final EnterpriseSerializationService ss;
        private final MemoryManager memoryManager;
        private final Queue<HiDensityNativeMemoryCacheRecord> recordQ = new ArrayDeque<HiDensityNativeMemoryCacheRecord>(1024);
        private final Queue<OffHeapData> dataQ = new ArrayDeque<OffHeapData>(1024);

        public CacheRecordAccessor(EnterpriseSerializationService ss) {
            this.ss = ss;
            this.memoryManager = ss.getMemoryManager();
        }

        @Override
        public boolean isEqual(long address, HiDensityNativeMemoryCacheRecord value) {
            return isEqual(address, value.address());
        }

        @Override
        public boolean isEqual(long address1, long address2) {
            long valueAddress1 = UnsafeHelper.UNSAFE.getLong(address1 + HiDensityNativeMemoryCacheRecord.VALUE_OFFSET);
            long valueAddress2 = UnsafeHelper.UNSAFE.getLong(address2 + HiDensityNativeMemoryCacheRecord.VALUE_OFFSET);
            return OffHeapDataUtil.equals(valueAddress1, valueAddress2);
        }

        public HiDensityNativeMemoryCacheRecord newRecord() {
            HiDensityNativeMemoryCacheRecord record = recordQ.poll();
            if (record == null) {
                record = new HiDensityNativeMemoryCacheRecord(this);
            }
            return record;
        }

        @Override
        public HiDensityNativeMemoryCacheRecord read(long address) {
            if (address <= NULL_PTR) {
                throw new IllegalArgumentException("Illegal memory address: " + address);
            }
            HiDensityNativeMemoryCacheRecord record = newRecord();
            record.reset(address);
            return record;
        }

        @Override
        public void dispose(HiDensityNativeMemoryCacheRecord record) {
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

        public <T> T readValue(HiDensityNativeMemoryCacheRecord record,
                               boolean enqueeDataOnFinish) {
            OffHeapData offHeapData = readData(record.getValueAddress());
            if (offHeapData != null) {
                try {
                    return (T) ss.toObject(offHeapData); //ss.convertData(offHeapData, DataType.HEAP);
                } finally {
                    if (enqueeDataOnFinish) {
                        enqueueData(offHeapData);
                    }
                }

            } else {
                return null;
            }
        }

        public void disposeValue(HiDensityNativeMemoryCacheRecord record) {
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

        void enqueueRecord(HiDensityNativeMemoryCacheRecord record) {
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
                ((EnterpriseCacheService) cacheService).sendInvalidationEvent(cacheConfig.getName(), object, "<NA>");
            }
        };
    }

    protected void onEntryInvalidated(Data key, String source) {
        ((EnterpriseCacheService) cacheService).sendInvalidationEvent(cacheConfig.getName(), key, source);
    }

    protected void onClear() {
        ((EnterpriseCacheService) cacheService).sendInvalidationEvent(cacheConfig.getName(), null, "<NA>");
    }

    protected void onDestroy() {
        ((EnterpriseCacheService) cacheService).sendInvalidationEvent(cacheConfig.getName(), null, "<NA>");
        ScheduledFuture<?> f = evictionTaskFuture;
        if (f != null) {
            f.cancel(true);
        }
    }

    public BinaryOffHeapHashMap<HiDensityNativeMemoryCacheRecord>.EntryIter iterator(int slot) {
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
