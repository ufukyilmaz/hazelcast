/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cache.enterprise.impl.hidensity.nativememory;

import com.hazelcast.cache.enterprise.EnterpriseCacheService;
import com.hazelcast.cache.enterprise.hidensity.HiDensityCacheRecordAccessor;
import com.hazelcast.cache.enterprise.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.enterprise.operation.CacheEvictionOperation;
import com.hazelcast.cache.impl.AbstractCacheRecordStore;
import com.hazelcast.cache.impl.CacheEntryProcessorEntry;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.elasticcollections.map.BinaryOffHeapHashMap;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.error.OffHeapOutOfMemoryError;
import com.hazelcast.monitor.LocalMemoryStats;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.OffHeapData;
import com.hazelcast.nio.serialization.OffHeapDataUtil;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.Clock;

import javax.cache.expiry.ExpiryPolicy;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ScheduledFuture;

/**
 * @author sozal 14/10/14
 */
public class HiDensityNativeMemoryCacheRecordStore
        extends AbstractCacheRecordStore<
                    HiDensityNativeMemoryCacheRecord,
                    HiDensityNativeMemoryCacheRecordMap>
        implements HiDensityCacheRecordStore<HiDensityNativeMemoryCacheRecord> {

    /**
     * Default value for initial capacity of Hi-Density Native Memory Cache Record Store
     */
    public static final int DEFAULT_INITIAL_CAPACITY = 1000;

    private final int initialCapacity;
    private final EnterpriseSerializationService serializationService;
    private final Operation evictionOperation;
    private MemoryManager memoryManager;
    private HiDensityNativeMemoryCacheRecordAccessor cacheRecordAccessor;
    private volatile boolean hasTtl;

    protected HiDensityNativeMemoryCacheRecordStore(final int partitionId,
                                                    final String name,
                                                    final EnterpriseCacheService cacheService,
                                                    final NodeEngine nodeEngine,
                                                    final int initialCapacity,
                                                    final ExpiryPolicy expiryPolicy,
                                                    final EvictionPolicy evictionPolicy,
                                                    final int evictionPercentage,
                                                    final int evictionThresholdPercentage,
                                                    final boolean evictionTaskEnable) {
        super(name, partitionId, nodeEngine, cacheService, expiryPolicy,
              evictionPolicy, evictionPercentage, evictionThresholdPercentage, evictionTaskEnable);
        this.initialCapacity = initialCapacity;
        this.serializationService = (EnterpriseSerializationService) nodeEngine.getSerializationService();

        this.cacheRecordAccessor = new HiDensityNativeMemoryCacheRecordAccessor(serializationService);
        this.memoryManager = serializationService.getMemoryManager();
        this.records = createRecordCacheMap();
        this.hasTtl = false;
        this.evictionOperation = createEvictionOperation(evictionPercentage);
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
             DEFAULT_EVICTION_THRESHOLD_PERCENTAGE,
             DEFAULT_IS_EVICTION_TASK_ENABLE);
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
            new HiDensityNativeMemoryCacheRecordMap(initialCapacity, serializationService,
                                                    cacheRecordAccessor, createEvictionCallback());
    }

    @Override
    protected CacheEntryProcessorEntry createCacheEntryProcessorEntry(Data key,
                                                                      HiDensityNativeMemoryCacheRecord record,
                                                                      long now) {
        return new HiDensityNativeMemoryCacheEntryProcessorEntry(key, record, this, now);
    }

    @Override
    protected HiDensityNativeMemoryCacheRecord createRecord(Object value,
                                                            long creationTime,
                                                            long expiryTime) {
        evictIfRequired();

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
            if (record != null && record.address() != NULL_PTR) {
                cacheRecordAccessor.dispose(record);
            }
            if (offHeapValue != null && offHeapValue.address() != NULL_PTR) {
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
        if (record.getValueAddress() == NULL_PTR) {
            return null;
        }
        return (T) cacheRecordAccessor.readValue(record, true);
    }

    @Override
    protected Data recordToData(HiDensityNativeMemoryCacheRecord record) {
        if (record.getValueAddress() == NULL_PTR) {
            return null;
        }
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

    @Override
    protected Data toData(Object obj) {
        if ((obj instanceof Data) && !(obj instanceof OffHeapData)) {
            return serializationService.convertData((Data) obj, DataType.OFFHEAP);
        } else {
            return super.toData(obj);
        }
    }

    @Override
    protected Data toHeapData(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Data) {
            Data data = (Data) obj;
            if (obj instanceof OffHeapData) {
                return serializationService.convertData(data, DataType.HEAP);
            } else {
                return data;
            }
        } else if (obj instanceof CacheRecord) {
            CacheRecord record = (CacheRecord) obj;
            Object value = record.getValue();
            return toHeapData(value);
        } else {
            return serializationService.toData(obj, DataType.HEAP);
        }
    }

    private OffHeapData getRecordData(HiDensityNativeMemoryCacheRecord record) {
        return cacheRecordAccessor.readData(record.getValueAddress());
    }

    private OffHeapData toOffHeapData(Object data) {
        OffHeapData offHeapData;
        if (!(data instanceof Data)) {
            offHeapData = serializationService.toData(data, DataType.OFFHEAP);
        } else if (!(data instanceof OffHeapData)) {
            offHeapData = serializationService.convertData((Data) data, DataType.OFFHEAP);
        } else {
            offHeapData = (OffHeapData) data;
        }
        return offHeapData;
    }

    @Override
    protected boolean isEvictionRequired() {
        LocalMemoryStats memoryStats = memoryManager.getMemoryStats();
        return (memoryStats.getMaxNativeMemory() * evictionThreshold)
                    > memoryStats.getFreeNativeMemory();
    }

    @Override
    public int forceEvict() {
        int percentage = Math.max(MIN_FORCED_EVICT_PERCENTAGE, evictionPercentage);
        int evicted = 0;
        if (hasTTL()) {
            evicted = records.evictExpiredRecords(ONE_HUNDRED_PERCENT);
        }
        evicted += records.evictRecords(percentage, EvictionPolicy.RANDOM);
        return evicted;
    }

    @Override
    public Object getDataValue(Data data) {
        if (data != null) {
            return serializationService.toObject(data);
        } else {
            return null;
        }
    }

    @Override
    public Object getRecordValue(HiDensityNativeMemoryCacheRecord record) {
        return getDataValue(getRecordData(record));
    }

    @Override
    public BinaryOffHeapHashMap<HiDensityNativeMemoryCacheRecord>.EntryIter iterator(int slot) {
        return records.iterator(slot);
    }

    @Override
    public void onAccess(long now,
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

    @Override
    public boolean hasTTL() {
        return hasTtl;
    }

    @Override
    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    @Override
    public EnterpriseCacheService getCacheService() {
        return (EnterpriseCacheService) cacheService;
    }

    @Override
    public HiDensityNativeMemoryCacheRecordAccessor getCacheRecordAccessor() {
        return cacheRecordAccessor;
    }

    @Override
    protected void onBeforeUpdateRecord(Data key,
                                        HiDensityNativeMemoryCacheRecord record,
                                        Object value,
                                        Data oldDataValue) {

    }

    @Override
    protected void onAfterUpdateRecord(Data key,
                                       HiDensityNativeMemoryCacheRecord record,
                                       Object value,
                                       Data oldDataValue) {
        if (oldDataValue != null && oldDataValue instanceof OffHeapData) {
            OffHeapData offHeapData = (OffHeapData) oldDataValue;
            if (offHeapData.address() != NULL_PTR) {
                cacheRecordAccessor.disposeData(offHeapData);
            }
        }
    }

    @Override
    protected void onUpdateRecordError(Data key,
                                       HiDensityNativeMemoryCacheRecord record,
                                       Object value,
                                       Data newDataValue,
                                       Data oldDataValue,
                                       Throwable error) {
        if (newDataValue != null && newDataValue instanceof OffHeapData) {
            OffHeapData offHeapData = (OffHeapData) newDataValue;
            if (offHeapData.address() != NULL_PTR) {
                cacheRecordAccessor.disposeData(offHeapData);
            }
        }
    }

    @Override
    protected void onDeleteRecord(Data key,
                                  HiDensityNativeMemoryCacheRecord record,
                                  Data dataValue,
                                  boolean deleted) {
        if (deleted && record != null) {
            cacheRecordAccessor.dispose(record);
        }
    }

    @Override
    protected void onDeleteRecordError(Data key,
                                       HiDensityNativeMemoryCacheRecord record,
                                       Data dataValue,
                                       Throwable error) {
        if (dataValue != null && dataValue instanceof OffHeapData) {
            OffHeapData offHeapData = (OffHeapData) dataValue;
            if (offHeapData.address() != NULL_PTR) {
                cacheRecordAccessor.disposeData(offHeapData);
            }
        }
    }

    @Override
    public void setRecord(Data key, CacheRecord record) {
        if (!(record instanceof HiDensityNativeMemoryCacheRecord)) {
            throw new IllegalArgumentException("record must be an instance of "
                    + HiDensityNativeMemoryCacheRecord.class.getName());
        }
        HiDensityNativeMemoryCacheRecord updatedRecord = records.get(key);
        records.set(key, (HiDensityNativeMemoryCacheRecord) record);
        if (updatedRecord != null && updatedRecord.getValueAddress() != NULL_PTR) {
            // Record itself is disposed by record map with new record,
            // so we should only dispose its value
            cacheRecordAccessor.disposeValue(updatedRecord);
        }
    }

    @Override
    public CacheRecord removeRecord(Data key) {
        HiDensityNativeMemoryCacheRecord deletedRecord = records.remove(key);
        if (deletedRecord != null) {
            cacheRecordAccessor.disposeValue(deletedRecord);
        }
        return deletedRecord;
    }

    /*
    protected void deleteRecord(Data key) {
        final HiDensityNativeMemoryCacheRecord record = records.remove(key);
        final OffHeapData dataValue = (OffHeapData) toData(record);
        if (isEventsEnabled) {
            publishEvent(cacheConfig.getName(),
                         CacheEventType.REMOVED,
                         key,
                         null,
                         dataValue,
                         false);
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
        // TODO avoid free() until read is completed!
        try {
            return serializationService.convertData(value, DataType.HEAP);
        } finally {
            if (value != null) {
                cacheRecordAccessor.enqueueData(value);
            }
        }
    }
    */

    //CHECKSTYLE:OFF
    private Data putInternal(Data key,
                             Object value,
                             ExpiryPolicy expiryPolicy,
                             boolean getValue,
                             String caller) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        long now = Clock.currentTimeMillis();
        evictIfRequired();

        long creationTime;
        HiDensityNativeMemoryCacheRecord record = null;
        OffHeapData newValue = null;
        Data oldValue = null;
        boolean newPut = false;

        try {
            record = records.get(key);
            if (record == null) {
                record = createRecord(null, now);
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
                // TODO avoid free() until read is completed!
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
    //CHECKSTYLE:ON

    //CHECKSTYLE:OFF
    @Override
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
        if (!willBeNewPut) {
            if (record == null || record.getValueAddress() == NULL_PTR) {
                throw new IllegalStateException("Invalid record -> " + record);
            }
        }
        if (record != null && record.getValueAddress() != NULL_PTR) {
            if (getValue) {
                OffHeapData current = cacheRecordAccessor.readData(record.getValueAddress());
                cacheRecordAccessor.disposeData(current);
                record.setValueAddress(NULL_PTR);
            }
            // This is commented-out since old data (value) of off-heap record
            // is already disposed by "onAfterUpdateRecord" method on record update
            /*
            else {
                cacheRecordAccessor.disposeValue(record);
            }
            */
        }
    }
    //CHECKSTYLE:ON

    //CHECKSTYLE:OFF
    @Override
    protected void onAfterGetAndPut(Data key,
                                    Object value,
                                    ExpiryPolicy expiryPolicy,
                                    String caller,
                                    boolean getValue,
                                    boolean disableWriteThrough,
                                    HiDensityNativeMemoryCacheRecord record,
                                    Object oldValue,
                                    boolean isExpired,
                                    boolean isNewPut,
                                    boolean isSaveSucceed) {
        /*
        expiryPolicy = getExpiryPolicy(expiryPolicy);
        long ttlMillis = expiryPolicyToTTL(expiryPolicy);
        ttlMillis = ttlMillis < Integer.MAX_VALUE ? ttlMillis : Integer.MAX_VALUE;
        if (!hasTtl && ttlMillis > 0) {
            hasTtl = true;
        }
        */
        if (record != null) {
            cacheRecordAccessor.enqueueRecord(record);
        }
    }
    //CHECKSTYLE:ON

    //CHECKSTYLE:OFF
    @Override
    protected void onGetAndPutError(Data key,
                                    Object value,
                                    ExpiryPolicy expiryPolicy,
                                    String caller,
                                    boolean getValue,
                                    boolean disableWriteThrough,
                                    HiDensityNativeMemoryCacheRecord record,
                                    Object oldValue,
                                    boolean wouldBeNewPut,
                                    Throwable error) {
        if (wouldBeNewPut && record != null && record.address() != NULL_PTR) {
            if (!records.delete(key)) {
                cacheRecordAccessor.dispose(record);
            }
        }
    }
    //CHECKSTYLE:ON

    @Override
    public void put(Data key, Object value, String caller) {
        put(key, value, defaultExpiryPolicy, caller);
    }

    /*
    @Override
    public void put(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        putInternal(key, value, expiryPolicy, false, caller);
    }

    @Override
    public Object getAndPut(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        return putInternal(key, value, expiryPolicy, true, caller);
    }
    */

    @Override
    public void own(Data key, Object value, long ttlMillis) {
        // TODO Implement without creating a "ExpirePolicy" object
        // This is just a quick workaround
        putInternal(key, value, ttlToExpirePolicy(ttlMillis), false, null);
    }

    @Override
    protected void onBeforePutIfAbsent(Data key,
                                       Object value,
                                       ExpiryPolicy expiryPolicy,
                                       String caller,
                                       boolean disableWriteThrough,
                                       HiDensityNativeMemoryCacheRecord record,
                                       boolean isExpired) {

    }

    @Override
    protected void onAfterPutIfAbsent(Data key,
                                      Object value,
                                      ExpiryPolicy expiryPolicy,
                                      String caller,
                                      boolean disableWriteThrough,
                                      HiDensityNativeMemoryCacheRecord record,
                                      boolean isExpired,
                                      boolean isSaveSucceed) {
        if (record != null) {
            cacheRecordAccessor.enqueueRecord(record);
        }
    }

    @Override
    protected void onPutIfAbsentError(Data key,
                                      Object value,
                                      ExpiryPolicy expiryPolicy,
                                      String caller,
                                      boolean disableWriteThrough,
                                      HiDensityNativeMemoryCacheRecord record,
                                      Throwable error) {
        if (record != null && record.address() != NULL_PTR) {
            if (!records.delete(key)) {
                cacheRecordAccessor.dispose(record);
            }
        }
        // This is commented-out since new data (value) of off-heap record
        // is already disposed by "onUpdateRecordError" method on record update
        /*
        if (newValue != null && newValue.address() != NULL_PTR) {
            cacheRecordAccessor.disposeData(newValue);
        }
        */
    }

    @Override
    public boolean putIfAbsent(Data key, Object value, String caller) {
        return putIfAbsent(key, value, defaultExpiryPolicy, caller);
    }

    /*
    @Override
    public boolean putIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        long now = Clock.currentTimeMillis();
        evictIfRequired();
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
    */

    @Override
    protected void onBeforeGetAndReplace(Data key,
                                         Object oldValue,
                                         Object newValue,
                                         ExpiryPolicy expiryPolicy,
                                         String caller,
                                         boolean getValue,
                                         HiDensityNativeMemoryCacheRecord record,
                                         boolean isExpired) {
        // This is commented-out since old data (value) of off-heap record
        // is already disposed by "onAfterUpdateRecord" method on record update
        /*
        if (record != null) {
            cacheRecordAccessor.disposeValue(record);
        }
        */
    }

    //CHECKSTYLE:OFF
    @Override
    protected void onAfterGetAndReplace(Data key,
                                        Object oldValue,
                                        Object newValue,
                                        ExpiryPolicy expiryPolicy,
                                        String caller,
                                        boolean getValue,
                                        HiDensityNativeMemoryCacheRecord record,
                                        boolean isExpired,
                                        boolean replaced) {
        if (record != null) {
            if (isExpired) {
                cacheRecordAccessor.dispose(record);
            } else {
                cacheRecordAccessor.enqueueRecord(record);
            }
        }
    }
    //CHECKSTYLE:ON

    @Override
    public boolean replace(Data key, Object value, String caller) {
        return replace(key, value, defaultExpiryPolicy, caller);
    }

    /*
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
    */

    @Override
    public boolean replace(Data key, Object oldValue, Object newValue, String caller) {
        return replace(key, oldValue, newValue, defaultExpiryPolicy, caller);
    }

    /*
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
    */

    @Override
    public Object getAndReplace(Data key, Object value, String caller) {
        return getAndReplace(key, value, defaultExpiryPolicy, caller);
    }

    /*
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
    */

    @Override
    protected void onRemove(Data key,
                            Object value,
                            String caller,
                            boolean getValue,
                            HiDensityNativeMemoryCacheRecord record,
                            boolean removed) {
        onEntryInvalidated(key, caller);
        if (record != null) {
            cacheRecordAccessor.enqueueRecord(record);
        }
    }

    /*
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
    */

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

    @Override
    public void clear() {
        onClear();
        records.clear();
    }

    @Override
    public void destroy() {
        clear();
        onDestroy();
        records.destroy();
    }

    protected void onClear() {
        ((EnterpriseCacheService) cacheService)
                .sendInvalidationEvent(cacheConfig.getName(), null, "<NA>");
    }

    protected void onDestroy() {
        ((EnterpriseCacheService) cacheService)
                .sendInvalidationEvent(cacheConfig.getName(), null, "<NA>");
        ScheduledFuture<?> f = evictionTaskFuture;
        if (f != null) {
            f.cancel(true);
        }
    }

    protected Callback<Data> createEvictionCallback() {
        return new Callback<Data>() {
            public void notify(Data object) {
                ((EnterpriseCacheService) cacheService)
                        .sendInvalidationEvent(cacheConfig.getName(), object, "<NA>");
            }
        };
    }

    protected void onEntryInvalidated(Data key, String source) {
        ((EnterpriseCacheService) cacheService)
                .sendInvalidationEvent(cacheConfig.getName(), key, source);
    }

    protected Operation createEvictionOperation(int percentage) {
        return new CacheEvictionOperation(cacheConfig.getName(), percentage)
                .setNodeEngine(nodeEngine)
                .setPartitionId(partitionId)
                .setCallerUuid(nodeEngine.getLocalMember().getUuid())
                .setService(cacheService);
    }

    @Override
    protected void onEvict() {
        if (hasTTL()) {
            OperationService operationService = nodeEngine.getOperationService();
            operationService.executeOperation(evictionOperation);
        }
    }

    /**
     * Cache record accessor for {@link HiDensityNativeMemoryCacheRecord}
     * for creating, reading, disposing record or its data.
     */
    public static class HiDensityNativeMemoryCacheRecordAccessor
            implements HiDensityCacheRecordAccessor<HiDensityNativeMemoryCacheRecord> {

        private final EnterpriseSerializationService ss;
        private final MemoryManager memoryManager;
        private final Queue<HiDensityNativeMemoryCacheRecord> recordQ = new ArrayDeque<HiDensityNativeMemoryCacheRecord>(1024);
        private final Queue<OffHeapData> dataQ = new ArrayDeque<OffHeapData>(1024);

        public HiDensityNativeMemoryCacheRecordAccessor(EnterpriseSerializationService ss) {
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

        @Override
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

        @Override
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

        @Override
        public Object readValue(HiDensityNativeMemoryCacheRecord record,
                                boolean enqueeDataOnFinish) {
            OffHeapData offHeapData = readData(record.getValueAddress());
            if (offHeapData != null) {
                try {
                    return ss.toObject(offHeapData);
                } finally {
                    if (enqueeDataOnFinish) {
                        enqueueData(offHeapData);
                    }
                }

            } else {
                return null;
            }
        }

        @Override
        public void disposeValue(HiDensityNativeMemoryCacheRecord record) {
            long valueAddress = record.getValueAddress();
            if (valueAddress != NULL_PTR) {
                disposeData(valueAddress);
                record.setValueAddress(NULL_PTR);
            }
        }

        @Override
        public void disposeData(OffHeapData value) {
            ss.disposeData(value);
            dataQ.offer(value);
        }

        @Override
        public void disposeData(long address) {
            OffHeapData data = readData(address);
            disposeData(data);
        }

        @Override
        public void enqueueRecord(HiDensityNativeMemoryCacheRecord record) {
            recordQ.offer(record.reset(NULL_PTR));
        }

        @Override
        public void enqueueData(OffHeapData data) {
            data.reset(NULL_PTR);
            dataQ.offer(data);
        }

    }

}
