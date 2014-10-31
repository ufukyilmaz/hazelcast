package com.hazelcast.cache.enterprise.impl.nativememory;

import com.hazelcast.cache.EnterpriseCacheService;

import com.hazelcast.cache.enterprise.BreakoutCacheRecordStore;
import com.hazelcast.cache.enterprise.operation.CacheEvictionOperation;
import com.hazelcast.cache.impl.AbstractCacheRecordStore;
import com.hazelcast.cache.impl.CacheEntryProcessorEntry;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.elastic.map.BinaryElasticHashMap;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.monitor.LocalMemoryStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.NativeMemoryData;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.Clock;

import javax.cache.expiry.ExpiryPolicy;
import java.util.concurrent.ScheduledFuture;

/**
 * @author sozal 14/10/14
 */
public class BreakoutNativeMemoryCacheRecordStore
        extends AbstractCacheRecordStore<BreakoutNativeMemoryCacheRecord, BreakoutNativeMemoryCacheRecordMap>
        implements BreakoutCacheRecordStore<BreakoutNativeMemoryCacheRecord> {

    /**
     * Default value for initial capacity of Native Memory Cache Record Store
     */
    public static final int DEFAULT_INITIAL_CAPACITY = 1000;

    private final int initialCapacity;
    private final float evictionThreshold;
    private final EnterpriseSerializationService serializationService;
    private final Operation evictionOperation;
    private MemoryManager memoryManager;
    private BreakoutNativeMemoryCacheRecordAccessor cacheRecordAccessor;

    protected BreakoutNativeMemoryCacheRecordStore(final int partitionId, final String name,
            final EnterpriseCacheService cacheService, final NodeEngine nodeEngine, final int initialCapacity,
            final EvictionPolicy evictionPolicy, final int evictionPercentage, final int evictionThresholdPercentage,
            final boolean evictionTaskEnable) {
        super(name, partitionId, nodeEngine, cacheService,
                evictionPolicy, evictionPercentage, evictionThresholdPercentage, evictionTaskEnable);
        this.initialCapacity = initialCapacity;
        this.evictionThreshold = (float) Math.max(1, ONE_HUNDRED_PERCENT - evictionThresholdPercentage)
                / ONE_HUNDRED_PERCENT;
        this.serializationService = (EnterpriseSerializationService) nodeEngine.getSerializationService();
        this.cacheRecordAccessor = new BreakoutNativeMemoryCacheRecordAccessor(serializationService);
        this.memoryManager = serializationService.getMemoryManager();
        this.records = createRecordCacheMap();
        this.evictionOperation = createEvictionOperation(evictionPercentage);
    }

    public BreakoutNativeMemoryCacheRecordStore(final int partitionId, final String cacheName,
            final EnterpriseCacheService cacheService, final NodeEngine nodeEngine, final int initialCapacity) {
        this(partitionId,
                cacheName,
                cacheService,
                nodeEngine,
                initialCapacity,
                null,
                DEFAULT_EVICTION_PERCENTAGE,
                DEFAULT_EVICTION_THRESHOLD_PERCENTAGE,
                DEFAULT_IS_EVICTION_TASK_ENABLE);
    }

    @Override
    protected BreakoutNativeMemoryCacheRecordMap createRecordCacheMap() {
        if (records != null) {
            return records;
        }
        if (serializationService == null || cacheRecordAccessor == null) {
            return null;
        }
        return
                new BreakoutNativeMemoryCacheRecordMap(initialCapacity, serializationService,
                        cacheRecordAccessor, createEvictionCallback());
    }

    @Override
    protected CacheEntryProcessorEntry createCacheEntryProcessorEntry(Data key,
                                                                      BreakoutNativeMemoryCacheRecord record,
                                                                      long now) {
        return new BreakoutNativeMemoryCacheEntryProcessorEntry(key, record, this, now);
    }

    @Override
    protected void updateHasExpiringEntry(BreakoutNativeMemoryCacheRecord record) {
        if (record != null && record.address() != NULL_PTR) {
            long ttlMillis = record.getTtlMillis();
            ttlMillis = ttlMillis < Integer.MAX_VALUE ? ttlMillis : Integer.MAX_VALUE;
            if (!hasExpiringEntry && ttlMillis >= 0) {
                hasExpiringEntry = true;
            }
        }
    }

    @Override
    protected BreakoutNativeMemoryCacheRecord createRecord(Object value,
                                                            long creationTime,
                                                            long expiryTime) {
        return createRecordInternal(value, creationTime, expiryTime, false, true);
    }

    private BreakoutNativeMemoryCacheRecord createRecordInternal(Object value,
                                                                  long creationTime,
                                                                  long expiryTime,
                                                                  boolean forceEvict,
                                                                  boolean retryOnOutOfMemoryError) {
        if (forceEvict) {
            forceEvict();
        } else {
            evictIfRequired();
        }

        NativeMemoryData offHeapValue = null;
        BreakoutNativeMemoryCacheRecord record = null;
        try {
            long address = memoryManager.allocate(BreakoutNativeMemoryCacheRecord.SIZE);
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
                record.setValueAddress(BreakoutNativeMemoryCacheRecordStore.NULL_PTR);
            }

            return record;
        } catch (NativeOutOfMemoryError e) {
            if (record != null && record.address() != NULL_PTR) {
                cacheRecordAccessor.dispose(record);
            }
            if (offHeapValue != null && offHeapValue.address() != NULL_PTR) {
                cacheRecordAccessor.disposeData(offHeapValue);
            }
            if (retryOnOutOfMemoryError) {
                return createRecordInternal(value, creationTime, expiryTime, true, false);
            } else {
                throw e;
            }
        }
    }

    @Override
    protected <T> Data valueToData(T value) {
        return serializationService.toData(value, DataType.NATIVE);
    }

    @Override
    protected <T> T dataToValue(Data data) {
        return (T) cacheService.toObject(data);
    }

    @Override
    protected <T> BreakoutNativeMemoryCacheRecord valueToRecord(T value) {
        return createRecord(value, -1, -1);
    }

    @Override
    protected <T> T recordToValue(BreakoutNativeMemoryCacheRecord record) {
        if (record.getValueAddress() == NULL_PTR) {
            return null;
        }
        return (T) cacheRecordAccessor.readValue(record, true);
    }

    @Override
    protected Data recordToData(BreakoutNativeMemoryCacheRecord record) {
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
    protected BreakoutNativeMemoryCacheRecord dataToRecord(Data data) {
        Object value = dataToValue(data);
        if (value == null) {
            return null;
        } else if (value instanceof BreakoutNativeMemoryCacheRecord) {
            return (BreakoutNativeMemoryCacheRecord) value;
        } else {
            return valueToRecord(value);
        }
    }

    @Override
    protected Data toData(Object obj) {
        if ((obj instanceof Data) && !(obj instanceof NativeMemoryData)) {
            return serializationService.convertData((Data) obj, DataType.NATIVE);
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
            if (obj instanceof NativeMemoryData) {
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

    private NativeMemoryData toOffHeapData(Object data) {
        NativeMemoryData nativeMemoryData;
        if (!(data instanceof Data)) {
            nativeMemoryData = serializationService.toData(data, DataType.NATIVE);
        } else if (!(data instanceof NativeMemoryData)) {
            nativeMemoryData = serializationService.convertData((Data) data, DataType.NATIVE);
        } else {
            nativeMemoryData = (NativeMemoryData) data;
        }
        return nativeMemoryData;
    }

    @Override
    public Object getRecordValue(BreakoutNativeMemoryCacheRecord record) {
        return recordToValue(record);
    }

    @Override
    protected boolean isEvictionRequired() {
        LocalMemoryStats memoryStats = memoryManager.getMemoryStats();
        return (memoryStats.getMaxNativeMemory() * evictionThreshold)
                > memoryStats.getFreeNativeMemory();
    }

    @Override
    public BinaryElasticHashMap<BreakoutNativeMemoryCacheRecord>.EntryIter iterator(int slot) {
        return records.iterator(slot);
    }

    @Override
    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    @Override
    public BreakoutNativeMemoryCacheRecordAccessor getCacheRecordAccessor() {
        return cacheRecordAccessor;
    }

    @Override
    protected void onBeforeUpdateRecord(Data key,
                                        BreakoutNativeMemoryCacheRecord record,
                                        Object value,
                                        Data oldDataValue) {

    }

    @Override
    protected void onAfterUpdateRecord(Data key,
                                       BreakoutNativeMemoryCacheRecord record,
                                       Object value,
                                       Data oldDataValue) {
        if (oldDataValue != null && oldDataValue instanceof NativeMemoryData) {
            NativeMemoryData nativeMemoryData = (NativeMemoryData) oldDataValue;
            if (nativeMemoryData.address() != NULL_PTR) {
                cacheRecordAccessor.disposeData(nativeMemoryData);
            }
        }
    }

    @Override
    protected void onUpdateRecordError(Data key,
                                       BreakoutNativeMemoryCacheRecord record,
                                       Object value,
                                       Data newDataValue,
                                       Data oldDataValue,
                                       Throwable error) {
        if (newDataValue != null && newDataValue instanceof NativeMemoryData) {
            NativeMemoryData nativeMemoryData = (NativeMemoryData) newDataValue;
            if (nativeMemoryData.address() != NULL_PTR) {
                cacheRecordAccessor.disposeData(nativeMemoryData);
            }
        }
    }

    @Override
    protected void onDeleteRecord(Data key,
                                  BreakoutNativeMemoryCacheRecord record,
                                  Data dataValue,
                                  boolean deleted) {
        if (deleted && record != null) {
            cacheRecordAccessor.dispose(record);
        }
    }

    @Override
    protected void onDeleteRecordError(Data key,
                                       BreakoutNativeMemoryCacheRecord record,
                                       Data dataValue,
                                       Throwable error) {
        if (dataValue != null && dataValue instanceof NativeMemoryData) {
            NativeMemoryData nativeMemoryData = (NativeMemoryData) dataValue;
            if (nativeMemoryData.address() != NULL_PTR) {
                cacheRecordAccessor.disposeData(nativeMemoryData);
            }
        }
    }

    @Override
    public void setRecord(Data key, CacheRecord record) {
        if (!(record instanceof BreakoutNativeMemoryCacheRecord)) {
            throw new IllegalArgumentException("record must be an instance of "
                    + BreakoutNativeMemoryCacheRecord.class.getName());
        }
        BreakoutNativeMemoryCacheRecord updatedRecord = records.get(key);
        records.set(key, (BreakoutNativeMemoryCacheRecord) record);
        if (updatedRecord != null && updatedRecord.getValueAddress() != NULL_PTR) {
            // Record itself is disposed by record map with new record,
            // so we should only dispose its value
            cacheRecordAccessor.disposeValue(updatedRecord);
        }
    }

    @Override
    public CacheRecord removeRecord(Data key) {
        BreakoutNativeMemoryCacheRecord deletedRecord = records.remove(key);
        if (deletedRecord != null) {
            cacheRecordAccessor.disposeValue(deletedRecord);
        }
        return deletedRecord;
    }

    protected void onGet(Data key,
                         ExpiryPolicy expiryPolicy,
                         Object value,
                         BreakoutNativeMemoryCacheRecord record) {
        cacheRecordAccessor.enqueueRecord(record);
    }


    private void onAccess(long now,
                          BreakoutNativeMemoryCacheRecord record,
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

    //CHECKSTYLE:OFF
    private Data putInternal(Data key,
                             Object value,
                             long ttlMillis,
                             boolean getValue,
                             String caller) {
        long now = Clock.currentTimeMillis();
        evictIfRequired();

        long creationTime;
        BreakoutNativeMemoryCacheRecord record = null;
        NativeMemoryData newValue = null;
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
                NativeMemoryData current = cacheRecordAccessor.readData(record.getValueAddress());
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

            ttlMillis = ttlMillis < Integer.MAX_VALUE ? ttlMillis : Integer.MAX_VALUE;
            if (!hasExpiringEntry && ttlMillis > 0) {
                hasExpiringEntry = true;
            }
            record.setTtlMillis((int) ttlMillis);

            cacheRecordAccessor.enqueueRecord(record);
        } catch (NativeOutOfMemoryError e) {
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
                                     BreakoutNativeMemoryCacheRecord record,
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
                NativeMemoryData current = cacheRecordAccessor.readData(record.getValueAddress());
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
                                    BreakoutNativeMemoryCacheRecord record,
                                    Object oldValue,
                                    boolean isExpired,
                                    boolean isNewPut,
                                    boolean isSaveSucceed) {
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
                                    BreakoutNativeMemoryCacheRecord record,
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

    @Override
    public void own(Data key, Object value, long ttlMillis) {
        putInternal(key, value, ttlMillis, false, null);
    }

    @Override
    protected void onBeforePutIfAbsent(Data key,
                                       Object value,
                                       ExpiryPolicy expiryPolicy,
                                       String caller,
                                       boolean disableWriteThrough,
                                       BreakoutNativeMemoryCacheRecord record,
                                       boolean isExpired) {

    }

    @Override
    protected void onAfterPutIfAbsent(Data key,
                                      Object value,
                                      ExpiryPolicy expiryPolicy,
                                      String caller,
                                      boolean disableWriteThrough,
                                      BreakoutNativeMemoryCacheRecord record,
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
                                      BreakoutNativeMemoryCacheRecord record,
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

    @Override
    protected void onBeforeGetAndReplace(Data key,
                                         Object oldValue,
                                         Object newValue,
                                         ExpiryPolicy expiryPolicy,
                                         String caller,
                                         boolean getValue,
                                         BreakoutNativeMemoryCacheRecord record,
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
                                        BreakoutNativeMemoryCacheRecord record,
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

    @Override
    public boolean replace(Data key, Object oldValue, Object newValue, String caller) {
        return replace(key, oldValue, newValue, defaultExpiryPolicy, caller);
    }

    @Override
    public Object getAndReplace(Data key, Object value, String caller) {
        return getAndReplace(key, value, defaultExpiryPolicy, caller);
    }


    @Override
    protected void onRemove(Data key,
                            Object value,
                            String caller,
                            boolean getValue,
                            BreakoutNativeMemoryCacheRecord record,
                            boolean removed) {
        onEntryInvalidated(key, caller);
        if (record != null) {
            cacheRecordAccessor.enqueueRecord(record);
        }
    }

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
        if (hasExpiringEntry) {
            OperationService operationService = nodeEngine.getOperationService();
            operationService.executeOperation(evictionOperation);
        }
    }

}
