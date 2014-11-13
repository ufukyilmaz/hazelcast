package com.hazelcast.cache.hidensity.impl.nativememory;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.hidensity.operation.CacheExpirationOperation;
import com.hazelcast.cache.impl.AbstractCacheRecordStore;
import com.hazelcast.cache.impl.CacheEntryProcessorEntry;
import com.hazelcast.cache.impl.CacheEventType;
import com.hazelcast.cache.impl.record.CacheDataRecord;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.elastic.map.BinaryElasticHashMap;
import com.hazelcast.memory.MemoryBlock;
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

/**
 * @author sozal 14/10/14
 */
public class HiDensityNativeMemoryCacheRecordStore
        extends AbstractCacheRecordStore<HiDensityNativeMemoryCacheRecord, HiDensityNativeMemoryCacheRecordMap>
        implements HiDensityCacheRecordStore<HiDensityNativeMemoryCacheRecord> {

    private static final int DEFAULT_EXPIRATION_PERCENTAGE = 10;

    private final int initialCapacity;
    private final float evictionThreshold;
    private final EnterpriseSerializationService serializationService;
    private final Operation expirationOperation;
    private final MemoryManager memoryManager;
    private final HiDensityNativeMemoryCacheRecordAccessor cacheRecordAccessor;

    public HiDensityNativeMemoryCacheRecordStore(int partitionId, String name,
                                                 EnterpriseCacheService cacheService, NodeEngine nodeEngine) {
        super(name, partitionId, nodeEngine, cacheService);

        this.initialCapacity = DEFAULT_INITIAL_CAPACITY;
        this.evictionThreshold = (float) Math.max(1, ONE_HUNDRED_PERCENT - evictionThresholdPercentage)
                / ONE_HUNDRED_PERCENT;
        this.serializationService = (EnterpriseSerializationService) nodeEngine.getSerializationService();
        this.cacheRecordAccessor = new HiDensityNativeMemoryCacheRecordAccessor(serializationService);
        this.memoryManager = serializationService.getMemoryManager();
        this.records = createRecordCacheMap();
        this.expirationOperation = createExpirationOperation(DEFAULT_EXPIRATION_PERCENTAGE);
    }

    @Override
    protected HiDensityNativeMemoryCacheRecordMap createRecordCacheMap() {
        if (records != null) {
            return records;
        }
        return new HiDensityNativeMemoryCacheRecordMap(initialCapacity, serializationService,
                        cacheRecordAccessor, createEvictionCallback());
    }

    @Override
    protected CacheEntryProcessorEntry createCacheEntryProcessorEntry(Data key,
            HiDensityNativeMemoryCacheRecord record, long now, int completionId) {
        return new HiDensityNativeMemoryCacheEntryProcessorEntry(key, record, this, now, completionId);
    }

    @Override
    protected void updateHasExpiringEntry(HiDensityNativeMemoryCacheRecord record) {
        if (record != null && record.address() != NULL_PTR) {
            long ttlMillis = record.getTtlMillis();
            ttlMillis = ttlMillis < Integer.MAX_VALUE ? ttlMillis : Integer.MAX_VALUE;
            if (!hasExpiringEntry && ttlMillis >= 0) {
                hasExpiringEntry = true;
            }
        }
    }

    private boolean isMemoryBlockValid(MemoryBlock memoryBlock) {
        return memoryBlock != null && memoryBlock.address() != NULL_PTR;
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
    protected <T> HiDensityNativeMemoryCacheRecord valueToRecord(T value) {
        return createRecord(value, -1, -1);
    }

    @Override
    protected <T> T recordToValue(HiDensityNativeMemoryCacheRecord record) {
        if (!isMemoryBlockValid(record)) {
            return null;
        }
        return (T) cacheRecordAccessor.readValue(record, true);
    }

    @Override
    protected Data recordToData(HiDensityNativeMemoryCacheRecord record) {
        if (!isMemoryBlockValid(record)) {
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

    private CacheRecord toHeapCacheRecord(HiDensityNativeMemoryCacheRecord record) {
        if (!isMemoryBlockValid(record)) {
            return null;
        }
        CacheRecord cacheRecord = new CacheDataRecord();
        cacheRecord.setCreationTime(record.getAccessTime());
        cacheRecord.setAccessTime(record.getAccessTime());
        cacheRecord.setAccessHit(record.getAccessHit());
        cacheRecord.setValue(toHeapData(record.getValue()));
        return cacheRecord;
    }

    private NativeMemoryData toNativeMemoryData(Object data) {
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
    public Object getRecordValue(HiDensityNativeMemoryCacheRecord record) {
        return recordToValue(record);
    }

    @Override
    protected boolean isEvictionRequired() {
        LocalMemoryStats memoryStats = memoryManager.getMemoryStats();
        return (memoryStats.getMaxNativeMemory() * evictionThreshold)
                    > memoryStats.getFreeNativeMemory();
    }

    @Override
    public BinaryElasticHashMap<HiDensityNativeMemoryCacheRecord>.EntryIter iterator(int slot) {
        return records.iterator(slot);
    }

    @Override
    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    @Override
    public HiDensityNativeMemoryCacheRecordAccessor getCacheRecordAccessor() {
        return cacheRecordAccessor;
    }

    @Override
    protected HiDensityNativeMemoryCacheRecord createRecord(Object value, long creationTime,
            long expiryTime) {
        return createRecordInternal(value, creationTime, expiryTime, false, true);
    }

    private HiDensityNativeMemoryCacheRecord createRecordInternal(Object value, long creationTime,
            long expiryTime, boolean forceEvict, boolean retryOnOutOfMemoryError) {
        if (forceEvict) {
            forceEvict();
        } else {
            evictIfRequired();
        }

        NativeMemoryData data = null;
        HiDensityNativeMemoryCacheRecord record;
        long recordAddress = NULL_PTR;

        try {
            recordAddress = memoryManager.allocate(HiDensityNativeMemoryCacheRecord.SIZE);
            record = cacheRecordAccessor.newRecord();
            record.reset(recordAddress);

            if (creationTime >= 0) {
                record.setCreationTime(creationTime);
            }
            if (expiryTime >= 0) {
                record.setExpirationTime(expiryTime);
            }
            if (value != null) {
                data = toNativeMemoryData(value);
                record.setValueAddress(data.address());
            } else {
                record.setValueAddress(NULL_PTR);
            }

            return record;
        } catch (NativeOutOfMemoryError e) {
            // If any memory region is allocated for record, dispose it
            if (recordAddress != NULL_PTR) {
                cacheRecordAccessor.dispose(recordAddress);
            }
            // If any data is allocated for record, dispose it
            if (isMemoryBlockValid(data)) {
                cacheRecordAccessor.disposeData(data);
            }
            if (retryOnOutOfMemoryError) {
                return createRecordInternal(value, creationTime, expiryTime, true, false);
            } else {
                throw e;
            }
        }
    }

    @Override
    protected void onUpdateRecord(Data key, HiDensityNativeMemoryCacheRecord record,
            Object value, Data oldDataValue) {
        // If there is valid old value, dispose it
        if (oldDataValue != null && oldDataValue instanceof NativeMemoryData) {
            NativeMemoryData nativeMemoryData = (NativeMemoryData) oldDataValue;
            if (isMemoryBlockValid(nativeMemoryData)) {
                cacheRecordAccessor.disposeData(nativeMemoryData);
            }
        }
    }

    @Override
    protected void onUpdateRecordError(Data key, HiDensityNativeMemoryCacheRecord record,
            Object value, Data newDataValue, Data oldDataValue, Throwable error) {
        boolean newValueInUse = false;
        // If new value is valid
        if (newDataValue != null && newDataValue instanceof NativeMemoryData) {
            NativeMemoryData nativeMemoryData = (NativeMemoryData) newDataValue;
            if (isMemoryBlockValid(nativeMemoryData)) {
                // Check about that if new value is in use or not for this record
                if (isMemoryBlockValid(record)
                        && record.getValueAddress() == nativeMemoryData.address()) {
                    newValueInUse = true;
                }
                // If new value is not in use, dispose its data since it is not used
                if (!newValueInUse) {
                    cacheRecordAccessor.disposeData(nativeMemoryData);
                }
            }
        }
        if (newValueInUse) {
            // If new value in used and old value is still valid, dispose it
            if (oldDataValue != null && oldDataValue instanceof NativeMemoryData) {
                NativeMemoryData nativeMemoryData = (NativeMemoryData) oldDataValue;
                if (isMemoryBlockValid(nativeMemoryData)) {
                    cacheRecordAccessor.disposeData(nativeMemoryData);
                }
            }
        } else {
            // If new value is not used and old value is still valid, restore old value of this record
            if (oldDataValue != null && oldDataValue instanceof NativeMemoryData) {
                NativeMemoryData nativeMemoryData = (NativeMemoryData) oldDataValue;
                if (isMemoryBlockValid(nativeMemoryData)) {
                    if (isMemoryBlockValid(record)) {
                        record.setValue(nativeMemoryData);
                    }
                }
            }
        }
    }

    @Override
    protected void onDeleteRecord(Data key, HiDensityNativeMemoryCacheRecord record,
            Data dataValue, boolean deleted) {
        // If record is deleted and if this record is valid, dispose it and its data
        if (deleted && isMemoryBlockValid(record)) {
            cacheRecordAccessor.dispose(record);
        }
    }

    @Override
    protected void onDeleteRecordError(Data key, HiDensityNativeMemoryCacheRecord record,
            Data dataValue, boolean deleted, Throwable error) {
        // If record is not deleted and its old value is still valid, restore old value of this record
        if (!deleted && isMemoryBlockValid(record)) {
            if (dataValue != null && dataValue instanceof NativeMemoryData) {
                NativeMemoryData nativeMemoryData = (NativeMemoryData) dataValue;
                if (isMemoryBlockValid(nativeMemoryData)) {
                    record.setValue(nativeMemoryData);
                }
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
        // If old record is valid, dispose it and its data
        if (isMemoryBlockValid(updatedRecord)) {
            cacheRecordAccessor.dispose(updatedRecord);
        }
    }

    @Override
    public CacheRecord removeRecord(Data key) {
        HiDensityNativeMemoryCacheRecord removedRecord = records.remove(key);
        CacheRecord recordToReturn = null;
        // If removed record is valid, first get a heap based copy of it and dispose it
        if (isMemoryBlockValid(removedRecord)) {
            recordToReturn = toHeapCacheRecord(removedRecord);
            cacheRecordAccessor.dispose(removedRecord);
        }
        return recordToReturn;
    }

    @Override
    protected void onGet(Data key, ExpiryPolicy expiryPolicy, Object value,
            HiDensityNativeMemoryCacheRecord record) {
        // If the record is available, put this to queue for reusing later
        if (record != null) {
            cacheRecordAccessor.enqueueRecord(record);
        }
    }

    @Override
    protected void onGetError(Data key, ExpiryPolicy expiryPolicy, Object value,
                              HiDensityNativeMemoryCacheRecord record, Throwable error) {
        // If the record is available, put this to queue for reusing later
        if (record != null) {
            cacheRecordAccessor.enqueueRecord(record);
        }
    }

    private void onAccess(long now, HiDensityNativeMemoryCacheRecord record,
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
    @Override
    protected void onPut(Data key, Object value, ExpiryPolicy expiryPolicy, String caller,
            boolean getValue, boolean disableWriteThrough, HiDensityNativeMemoryCacheRecord record,
            Object oldValue, boolean isExpired, boolean isNewPut, boolean isSaveSucceed) {
        // If the record is available, put this to queue for reusing later
        if (record != null) {
            cacheRecordAccessor.enqueueRecord(record);
        }
    }
    //CHECKSTYLE:ON

    //CHECKSTYLE:OFF
    @Override
    protected void onPutError(Data key, Object value, ExpiryPolicy expiryPolicy, String caller,
            boolean getValue, boolean disableWriteThrough, HiDensityNativeMemoryCacheRecord record,
            Object oldValue, boolean wouldBeNewPut, Throwable error) {
        // If this record has been somehow saved, dispose it
        if (wouldBeNewPut && isMemoryBlockValid(record)) {
            if (!records.delete(key)) {
                cacheRecordAccessor.dispose(record);
                return;
            }
        }
        // If the record is available, put this to queue for reusing later
        if (record != null) {
            cacheRecordAccessor.enqueueRecord(record);
        }
    }
    //CHECKSTYLE:ON

    @Override
    public void put(Data key, Object value, String caller, int completionId) {
        put(key, value, defaultExpiryPolicy, caller, completionId);
    }

    //CHECKSTYLE:OFF
    @Override
    public void own(Data key, Object value, long ttlMillis) {
        evictIfRequired();

        long now = Clock.currentTimeMillis();
        long creationTime;
        HiDensityNativeMemoryCacheRecord record = null;
        NativeMemoryData newDataValue = null;
        boolean newPut = false;

        try {
            record = records.get(key);
            if (record == null) {
                record = createRecord(null, now);
                creationTime = now;
                records.set(key, record);
                newPut = true;
            } else {
                creationTime = record.getCreationTime();
            }

            // Create data for new value
            newDataValue = toNativeMemoryData(value);

            // Dispose old value if exist
            if (record.getValueAddress() != NULL_PTR) {
                cacheRecordAccessor.disposeValue(record);
            }

            // Assign new value to record
            record.setValue(newDataValue);

            onAccess(now, record, creationTime);
            if (newPut) {
                record.resetAccessHit();
            }

            ttlMillis = ttlMillis < Integer.MAX_VALUE ? ttlMillis : Integer.MAX_VALUE;
            if (!hasExpiringEntry && ttlMillis > 0) {
                hasExpiringEntry = true;
            }
            record.setTtlMillis((int) ttlMillis);

            // Put this record to queue for reusing later
            cacheRecordAccessor.enqueueRecord(record);
        } catch (NativeOutOfMemoryError e) {
            // If this record has been somehow saved, dispose it
            if (newPut && isMemoryBlockValid(record)) {
                if (!records.delete(key)) {
                    cacheRecordAccessor.dispose(record);
                }
            }
            throw e;
        }
    }
    //CHECKSTYLE:ON

    @Override
    protected void onPutIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy, String caller,
            boolean disableWriteThrough, HiDensityNativeMemoryCacheRecord record,
            boolean isExpired, boolean isSaveSucceed) {
        // If the record is available, put this to queue for reusing later
        if (record != null) {
            cacheRecordAccessor.enqueueRecord(record);
        }
    }

    @Override
    protected void onPutIfAbsentError(Data key, Object value, ExpiryPolicy expiryPolicy, String caller,
            boolean disableWriteThrough, HiDensityNativeMemoryCacheRecord record, Throwable error) {
        // If this record has been somehow saved, dispose it
        if (isMemoryBlockValid(record)) {
            if (!records.delete(key)) {
                cacheRecordAccessor.dispose(record);
                return;
            }
        }
        // If the record is available, put this to queue for reusing later
        if (record != null) {
            cacheRecordAccessor.enqueueRecord(record);
        }
    }

    @Override
    public boolean putIfAbsent(Data key, Object value, String caller, int completionId) {
        return putIfAbsent(key, value, defaultExpiryPolicy, caller, completionId);
    }

    //CHECKSTYLE:OFF
    @Override
    protected void onReplace(Data key, Object oldValue, Object newValue, ExpiryPolicy expiryPolicy,
            String caller, boolean getValue, HiDensityNativeMemoryCacheRecord record,
            boolean isExpired, boolean replaced) {
        // If record is valid and expired, dispose it
        if (isExpired && isMemoryBlockValid(record)) {
            cacheRecordAccessor.dispose(record);
            return;
        }
        // If the record is available, put this to queue for reusing later
        if (record != null) {
            cacheRecordAccessor.enqueueRecord(record);
        }
    }
    //CHECKSTYLE:ON

    //CHECKSTYLE:OFF
    @Override
    protected void onReplaceError(Data key, Object oldValue, Object newValue, ExpiryPolicy expiryPolicy,
            String caller, boolean getValue, HiDensityNativeMemoryCacheRecord record,
            boolean isExpired, boolean replaced, Throwable error) {
        // If record is valid and expired, dispose it
        if (isExpired && isMemoryBlockValid(record)) {
            cacheRecordAccessor.dispose(record);
            return;
        }
        // If the record is available, put this to queue for reusing later
        if (record != null) {
            cacheRecordAccessor.enqueueRecord(record);
        }
    }
    //CHECKSTYLE:ON

    @Override
    public boolean replace(Data key, Object value, String caller, int completionId) {
        return replace(key, value, defaultExpiryPolicy, caller, completionId);
    }

    @Override
    public boolean replace(Data key, Object oldValue, Object newValue, String caller, int completionId) {
        return replace(key, oldValue, newValue, defaultExpiryPolicy, caller, completionId);
    }

    @Override
    public Object getAndReplace(Data key, Object value, String caller, int completionId) {
        return getAndReplace(key, value, defaultExpiryPolicy, caller, completionId);
    }

    @Override
    protected void onRemove(Data key, Object value, String caller, boolean getValue,
            HiDensityNativeMemoryCacheRecord record, boolean removed) {
        onEntryInvalidated(key, caller);
        // If the record is available, put this to queue for reusing later
        if (record != null) {
            cacheRecordAccessor.enqueueRecord(record);
        }
    }

    @Override
    protected void onRemoveError(Data key, Object value, String caller, boolean getValue,
                                 HiDensityNativeMemoryCacheRecord record, boolean removed,
                                 Throwable error) {
        // If record has been somehow removed and if it is still valid, dispose it and its data
        if (removed && isMemoryBlockValid(record)) {
            cacheRecordAccessor.dispose(record);
            return;
        }
        // If the record is available, put this to queue for reusing later
        if (record != null) {
            cacheRecordAccessor.enqueueRecord(record);
        }
    }

    @Override
    public void publishCompletedEvent(String cacheName, int completionId,
                                      Data dataKey, int orderKey) {
        if (completionId > 0) {
            cacheService
                    .publishEvent(cacheName, CacheEventType.COMPLETED, dataKey,
                            cacheService.toData(completionId), null, false, orderKey, completionId);
        }
    }

    @Override
    public void clear() {
        super.clear();
        onClear();
    }

    @Override
    public void destroy() {
        super.destroy();
        onDestroy();
    }

    protected void onClear() {
        records.clear();
        ((EnterpriseCacheService) cacheService)
                .sendInvalidationEvent(cacheConfig.getName(), null, "<NA>");
    }

    protected void onDestroy() {
        records.destroy();
        ((EnterpriseCacheService) cacheService)
                .sendInvalidationEvent(cacheConfig.getName(), null, "<NA>");
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

    protected Operation createExpirationOperation(int percentage) {
        return
            new CacheExpirationOperation(cacheConfig.getName(), percentage)
                    .setNodeEngine(nodeEngine)
                    .setPartitionId(partitionId)
                    .setCallerUuid(nodeEngine.getLocalMember().getUuid())
                    .setService(cacheService);
    }

    @Override
    protected void onExpiry() {
        if (hasExpiringEntry) {
            OperationService operationService = nodeEngine.getOperationService();
            operationService.executeOperation(expirationOperation);
        }
    }

}
