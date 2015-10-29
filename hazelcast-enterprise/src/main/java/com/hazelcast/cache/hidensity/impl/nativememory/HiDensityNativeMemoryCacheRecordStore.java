package com.hazelcast.cache.hidensity.impl.nativememory;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.hidensity.maxsize.HiDensityFreeNativeMemoryPercentageMaxSizeChecker;
import com.hazelcast.cache.hidensity.maxsize.HiDensityFreeNativeMemorySizeMaxSizeChecker;
import com.hazelcast.cache.hidensity.maxsize.HiDensityUsedNativeMemoryPercentageMaxSizeChecker;
import com.hazelcast.cache.hidensity.maxsize.HiDensityUsedNativeMemorySizeMaxSizeChecker;
import com.hazelcast.cache.impl.AbstractCacheRecordStore;
import com.hazelcast.cache.impl.CacheEntryProcessorEntry;
import com.hazelcast.cache.impl.maxsize.MaxSizeChecker;
import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.impl.merge.entry.DefaultCacheEntryView;
import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.impl.record.CacheDataRecord;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.elastic.SlottableIterator;
import com.hazelcast.hidensity.HiDensityRecordProcessor;
import com.hazelcast.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.impl.CacheEventContextUtil.createCacheCompleteEvent;

/**
 * @author sozal 14/10/14
 */
public class HiDensityNativeMemoryCacheRecordStore
        extends AbstractCacheRecordStore<HiDensityNativeMemoryCacheRecord, HiDensityNativeMemoryCacheRecordMap>
        implements HiDensityCacheRecordStore<HiDensityNativeMemoryCacheRecord> {

    // DEFAULT_INITIAL_CAPACITY;
    private static final int NATIVE_MEMORY_DEFAULT_INITIAL_CAPACITY = 256;

    private HiDensityStorageInfo cacheInfo;
    private EnterpriseSerializationService serializationService;
    private MemoryManager memoryManager;
    private HiDensityRecordProcessor<HiDensityNativeMemoryCacheRecord> cacheRecordProcessor;

    public HiDensityNativeMemoryCacheRecordStore(int partitionId, String name,
                                                 EnterpriseCacheService cacheService, NodeEngine nodeEngine) {
        super(name, partitionId, nodeEngine, cacheService);
        ensureInitialized();
    }

    private boolean isInvalidMaxSizePolicyExceptionDisabled() {
        // By default this property is not exist.
        // This property can be set to "true" for such as TCK tests.
        // Because there is no max-size policy.
        // For example: -DdisableInvalidMaxSizePolicyException=true
        return Boolean.getBoolean(SYSTEM_PROPERTY_NAME_TO_DISABLE_INVALID_MAX_SIZE_POLICY_EXCEPTION);
    }

    //CHECKSTYLE:OFF
    @Override
    protected MaxSizeChecker createCacheMaxSizeChecker(int size, EvictionConfig.MaxSizePolicy maxSizePolicy) {
        if (maxSizePolicy == null) {
            if (isInvalidMaxSizePolicyExceptionDisabled()) {
                // Don't throw exception, just ignore max-size policy
                return null;
            } else {
                throw new IllegalArgumentException("Max-Size policy cannot be null");
            }
        }

        final long maxNativeMemory =
                ((EnterpriseSerializationService) nodeEngine.getSerializationService())
                        .getMemoryManager().getMemoryStats().getMaxNativeMemory();
        switch (maxSizePolicy) {
            case USED_NATIVE_MEMORY_SIZE:
                return new HiDensityUsedNativeMemorySizeMaxSizeChecker(cacheInfo, size);
            case USED_NATIVE_MEMORY_PERCENTAGE:
                return new HiDensityUsedNativeMemoryPercentageMaxSizeChecker(cacheInfo, size, maxNativeMemory);
            case FREE_NATIVE_MEMORY_SIZE:
                return new HiDensityFreeNativeMemorySizeMaxSizeChecker(memoryManager, size);
            case FREE_NATIVE_MEMORY_PERCENTAGE:
                return new HiDensityFreeNativeMemoryPercentageMaxSizeChecker(memoryManager, size, maxNativeMemory);
            default:
                if (isInvalidMaxSizePolicyExceptionDisabled()) {
                    // Don't throw exception, just ignore max-size policy
                    return null;
                } else {
                    throw new IllegalArgumentException("Invalid max-size policy "
                            + "(" + maxSizePolicy + ") for " + getClass().getName() + " ! Only "
                            + EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE + ", "
                            + EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE + ", "
                            + EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE + ", "
                            + EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE
                            + " are supported.");
                }
        }
    }
    //CHECKSTYLE:ON

    @SuppressWarnings("unchecked")
    private void ensureInitialized() {
        if (cacheInfo == null) {
            cacheInfo = ((EnterpriseCacheService) cacheService)
                    .getOrCreateHiDensityCacheInfo(cacheConfig.getNameWithPrefix());
        }
        if (serializationService == null) {
            serializationService = (EnterpriseSerializationService) nodeEngine.getSerializationService();
        }
        if (memoryManager == null) {
            memoryManager = serializationService.getMemoryManager();
            if (memoryManager instanceof PoolingMemoryManager) {
                // `HiDensityNativeMemoryCacheRecordStore` is partition specific and
                // it is created from its partition specific thread.
                // Since memory manager is `PoolingMemoryManager`, this means that
                // this record store will always use its partition specific `ThreadLocalPoolingMemoryManager`.
                // In this case, no need to find its partition specific `ThreadLocalPoolingMemoryManager` inside
                // `GlobalPoolingMemoryManager` for every memory allocation/free every time.
                // So, we explicitly use its partition specific `ThreadLocalPoolingMemoryManager` directly.
                memoryManager = ((PoolingMemoryManager) memoryManager).getMemoryManager();
            }
        }
        if (memoryManager == null) {
            throw new IllegalStateException("Native memory must be enabled to use Hi-Density storage !");
        }
        if (cacheRecordProcessor == null) {
            cacheRecordProcessor =
                    new CacheHiDensityRecordProcessor(
                            serializationService,
                            new HiDensityNativeMemoryCacheRecordAccessor(serializationService, memoryManager),
                            memoryManager, cacheInfo);
        }
    }

    @Override
    protected HiDensityNativeMemoryCacheRecordMap createRecordCacheMap() {
        if (records != null) {
            return records;
        }
        ensureInitialized();

        HiDensityNativeMemoryCacheRecordMap cacheRecordMap = null;
        int capacity = NATIVE_MEMORY_DEFAULT_INITIAL_CAPACITY;
        NativeOutOfMemoryError oome = null;

        do {
            try {
                cacheRecordMap = new HiDensityNativeMemoryCacheRecordMap(capacity, cacheRecordProcessor, cacheInfo);
                break;
            } catch (NativeOutOfMemoryError e) {
                oome = e;
            }
            capacity = capacity >> 1;
        } while (capacity > 0);

        if (cacheRecordMap == null && oome != null) {
            throw oome;
        }
        return cacheRecordMap;
    }

    @Override
    protected CacheEntryProcessorEntry createCacheEntryProcessorEntry(Data key,
                                                                      HiDensityNativeMemoryCacheRecord record,
                                                                      long now, int completionId) {
        return new HiDensityNativeMemoryCacheEntryProcessorEntry(key, record, this, now, completionId);
    }

    private boolean isMemoryBlockValid(MemoryBlock memoryBlock) {
        return memoryBlock != null && memoryBlock.address() != NULL_PTR;
    }

    @Override
    protected Data valueToData(Object value) {
        return cacheRecordProcessor.toData(value, DataType.NATIVE);
    }

    @Override
    protected Object dataToValue(Data data) {
        return cacheService.toObject(data);
    }

    @Override
    protected Object recordToValue(HiDensityNativeMemoryCacheRecord record) {
        if (!isMemoryBlockValid(record)) {
            return null;
        }
        NativeMemoryData valueData = record.getValue();
        return cacheRecordProcessor.convertData(valueData, DataType.HEAP);
    }

    @Override
    protected Data recordToData(HiDensityNativeMemoryCacheRecord record) {
        if (!isMemoryBlockValid(record)) {
            return null;
        }
        return record.getValue();
    }

    @Override
    protected Data toData(Object obj) {
        if (obj instanceof Data) {
            if (obj instanceof NativeMemoryData) {
                return (NativeMemoryData) obj;
            } else {
                return cacheRecordProcessor.convertData((Data) obj, DataType.NATIVE);
            }
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
                return cacheRecordProcessor.convertData(data, DataType.HEAP);
            } else {
                return data;
            }
        } else if (obj instanceof CacheRecord) {
            CacheRecord record = (CacheRecord) obj;
            Object value = record.getValue();
            return toHeapData(value);
        } else {
            return cacheRecordProcessor.toData(obj, DataType.HEAP);
        }
    }

    private CacheRecord toHeapCacheRecord(HiDensityNativeMemoryCacheRecord record) {
        if (!isMemoryBlockValid(record)) {
            return null;
        }
        CacheRecord cacheRecord = new CacheDataRecord();
        cacheRecord.setCreationTime(record.getCreationTime());
        cacheRecord.setAccessTime(record.getAccessTime());
        cacheRecord.setAccessHit(record.getAccessHit());
        cacheRecord.setExpirationTime(record.getExpirationTime());
        cacheRecord.setValue(toHeapData(record.getValue()));
        return cacheRecord;
    }

    private HiDensityNativeMemoryCacheRecord toNativeMemoryCacheRecord(CacheRecord record) {
        if (record instanceof HiDensityNativeMemoryCacheRecord) {
            return (HiDensityNativeMemoryCacheRecord) record;
        }
        HiDensityNativeMemoryCacheRecord nativeMemoryRecord =
                createRecord(record.getValue(), record.getCreationTime(), record.getExpirationTime());
        nativeMemoryRecord.setAccessTime(record.getAccessTime());
        nativeMemoryRecord.setAccessHit(record.getAccessHit());
        return nativeMemoryRecord;
    }

    private NativeMemoryData toNativeMemoryData(Object data) {
        NativeMemoryData nativeMemoryData;
        if (!(data instanceof Data)) {
            nativeMemoryData = (NativeMemoryData) cacheRecordProcessor.toData(data, DataType.NATIVE);
        } else if (!(data instanceof NativeMemoryData)) {
            nativeMemoryData = (NativeMemoryData) cacheRecordProcessor.convertData((Data) data, DataType.NATIVE);
        } else {
            nativeMemoryData = (NativeMemoryData) data;
        }
        return nativeMemoryData;
    }

    @Override
    public Object getRecordValue(HiDensityNativeMemoryCacheRecord record) {
        return serializationService.toObject(record.getValue());
    }

    @Override
    public SlottableIterator<Map.Entry<Data, HiDensityNativeMemoryCacheRecord>> iterator(int slot) {
        return records.iterator(slot);
    }

    @Override
    public HiDensityRecordProcessor<HiDensityNativeMemoryCacheRecord> getRecordProcessor() {
        return cacheRecordProcessor;
    }

    @Override
    protected HiDensityNativeMemoryCacheRecord createRecord(Object value, long creationTime, long expiryTime) {
        return createRecordInternal(value, creationTime, expiryTime);
    }

    private HiDensityNativeMemoryCacheRecord createRecordInternal(Object value, long creationTime, long expiryTime) {
        NativeMemoryData data = null;
        HiDensityNativeMemoryCacheRecord record;
        long recordAddress = NULL_PTR;

        try {
            recordAddress = cacheRecordProcessor.allocate(HiDensityNativeMemoryCacheRecord.SIZE);
            record = cacheRecordProcessor.newRecord();
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
                cacheRecordProcessor.dispose(recordAddress);
            }
            // If any data is allocated for record, dispose it
            if (isMemoryBlockValid(data)) {
                cacheRecordProcessor.disposeData(data);
            }

            throw e;
        }
    }

    @Override
    //CHECKSTYLE:OFF
    protected void onCreateRecordWithExpiryError(Data key, Object value, long expiryTime, long now,
                                                 boolean disableWriteThrough, int completionId, String origin,
                                                 HiDensityNativeMemoryCacheRecord record, Throwable error) {
        if (isMemoryBlockValid(record)) {
            if (value instanceof NativeMemoryData) {
                // If value is allocated outside of record store, disposing value is its responsibility.
                // So just dispose record which is allocated here.
                cacheRecordProcessor.free(record.address(), record.size());
                record.reset(NULL_PTR);
            } else {
                cacheRecordProcessor.dispose(record);
            }
        }
    }
    //CHECKSTYLE:ON

    @Override
    protected void onProcessExpiredEntry(Data key, HiDensityNativeMemoryCacheRecord record, long expiryTime,
                                         long now, String source, String origin) {
        if (isMemoryBlockValid(record)) {
            cacheRecordProcessor.dispose(record);
        }
    }

    @Override
    protected void onUpdateRecord(Data key, HiDensityNativeMemoryCacheRecord record,
                                  Object value, Data oldDataValue) {
        // If there is valid old value, dispose it
        if (oldDataValue != null && oldDataValue instanceof NativeMemoryData) {
            NativeMemoryData nativeMemoryData = (NativeMemoryData) oldDataValue;
            if (isMemoryBlockValid(nativeMemoryData)) {
                cacheRecordProcessor.disposeData(nativeMemoryData);
            }
        }
    }

    @Override
    protected void onUpdateRecordError(Data key, HiDensityNativeMemoryCacheRecord record,
                                       Object value, Data newDataValue, Data oldDataValue, Throwable error) {
        // If there is valid new value and it is created at here, dispose it
        if (newDataValue != null && newDataValue instanceof NativeMemoryData && newDataValue != value) {
            NativeMemoryData nativeMemoryData = (NativeMemoryData) newDataValue;
            if (isMemoryBlockValid(nativeMemoryData)) {
                cacheRecordProcessor.disposeData(nativeMemoryData);
            }
        }
    }

    @Override
    protected void onDeleteRecord(Data key, HiDensityNativeMemoryCacheRecord record,
                                  Data dataValue, boolean deleted) {
        // If record is deleted and if this record is valid, dispose it and its data
        if (deleted && isMemoryBlockValid(record)) {
            cacheRecordProcessor.dispose(record);
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

    private HiDensityNativeMemoryCacheRecord putRecordInternal(Data key, CacheRecord record) {
        if (!records.containsKey(key)) {
            evictIfRequired();
        }

        HiDensityNativeMemoryCacheRecord newRecord = toNativeMemoryCacheRecord(record);
        HiDensityNativeMemoryCacheRecord oldRecord = null;
        try {
            oldRecord = doPutRecord(key, newRecord);
        } catch (Throwable t) {
            if (newRecord != record) {
                // If record is created at here, dispose it before throwing error
                cacheRecordProcessor.dispose(newRecord);
            }
            throw ExceptionUtil.rethrow(t);
        }

        // No exception means that put is successful.

        // Add key to deferred list if it is `NativeMemoryData`, then operation will dispose it later.
        // Because since old record is null, this means that put was handled as replace.
        // So key is not stored actually.
        if (oldRecord != null && key instanceof NativeMemoryData) {
            NativeMemoryData nativeKey = (NativeMemoryData) key;
            if (isMemoryBlockValid(nativeKey)) {
                cacheRecordProcessor.addDeferredDispose(nativeKey);
            }
        }

        // If old record is not exist (null) and there is no convertion for key,
        // add key memory usage to used memory explicitly.
        if (oldRecord == null && key instanceof NativeMemoryData) {
            long size = cacheRecordProcessor.getSize((NativeMemoryData) key);
            cacheRecordProcessor.increaseUsedMemory(size);
        }

        // If the new record is already native memory based, this means that there is no convertion,
        // add record and value memory usage to used memory explicitly.
        if (record instanceof HiDensityNativeMemoryCacheRecord) {
            long size = cacheRecordProcessor.getSize(newRecord);
            size += cacheRecordProcessor.getSize(newRecord.getValue());
            cacheRecordProcessor.increaseUsedMemory(size);
        }

        return oldRecord;
    }

    @Override
    public void putRecord(Data key, CacheRecord record) {
        HiDensityNativeMemoryCacheRecord oldRecord = putRecordInternal(key, record);
        if (isMemoryBlockValid(oldRecord)) {
            cacheRecordProcessor.dispose(oldRecord);
        }
    }

    @Override
    public CacheRecord removeRecord(Data key) {
        HiDensityNativeMemoryCacheRecord removedRecord = records.remove(key);
        CacheRecord recordToReturn = null;
        // If removed record is valid, first get a heap based copy of it and dispose it
        if (isMemoryBlockValid(removedRecord)) {
            recordToReturn = toHeapCacheRecord(removedRecord);
            cacheRecordProcessor.dispose(removedRecord);
        }
        return recordToReturn;
    }

    private void onAccess(long now, HiDensityNativeMemoryCacheRecord record, long creationTime) {
        if (isEvictionEnabled()) {
            if (evictionConfig.getEvictionPolicy() == EvictionPolicy.LRU) {
                long longDiff = now - creationTime;
                int diff = longDiff < Integer.MAX_VALUE ? (int) longDiff : Integer.MAX_VALUE;
                record.setAccessTimeDiff(diff);
            } else if (evictionConfig.getEvictionPolicy() == EvictionPolicy.LFU) {
                record.incrementAccessHit();
            }
        }
    }

    //CHECKSTYLE:OFF
    @Override
    protected void onPut(Data key, Object value, ExpiryPolicy expiryPolicy, String caller,
                         boolean getValue, boolean disableWriteThrough, HiDensityNativeMemoryCacheRecord record,
                         Object oldValue, boolean isExpired, boolean isNewPut, boolean isSaveSucceed) {
        // Old value is disposed at `onUpdateRecord`

        // If put is successful
        if (isSaveSucceed) {
            // If key is `NativeMemoryData`, since there is no convertion,
            // add its memory to used memory explicitly.
            if (isNewPut && key instanceof NativeMemoryData) {
                long size = cacheRecordProcessor.getSize((NativeMemoryData) key);
                cacheRecordProcessor.increaseUsedMemory(size);
            }
            // If value is `NativeMemoryData`, since there is no convertion,
            // add its memory to used memory explicitly.
            if (value instanceof NativeMemoryData) {
                long size = cacheRecordProcessor.getSize((NativeMemoryData) value);
                cacheRecordProcessor.increaseUsedMemory(size);
            }
        }

        if (isSaveSucceed) {
            if (!isNewPut) {
                if (key instanceof NativeMemoryData) {
                    // If save is successful as new put, this means that key is not stored.
                    // So add key to deferred list if it is `NativeMemoryData`, then operation will dispose it later.
                    NativeMemoryData nativeKey = (NativeMemoryData) key;
                    if (isMemoryBlockValid(nativeKey)) {
                        cacheRecordProcessor.addDeferredDispose(nativeKey);
                    }
                }
            }
        } else {
            // If save is not successful, this means that key is not stored.
            // So add key to deferred list if it is `NativeMemoryData`, then operation will dispose it later.
            if (key instanceof NativeMemoryData) {
                NativeMemoryData nativeKey = (NativeMemoryData) key;
                if (isMemoryBlockValid(nativeKey)) {
                    cacheRecordProcessor.addDeferredDispose(nativeKey);
                }
            }
            // If save is not successful, this means that value is not stored.
            // So add key to deferred list if it is `NativeMemoryData`, then operation will dispose it later.
            if (value instanceof NativeMemoryData) {
                NativeMemoryData nativeValue = (NativeMemoryData) value;
                if (isMemoryBlockValid(nativeValue)) {
                    cacheRecordProcessor.addDeferredDispose(nativeValue);
                }
            }
        }
    }
    //CHECKSTYLE:ON

    protected void onOwn(Data key, Object value, long ttlMillis, HiDensityNativeMemoryCacheRecord record,
                         NativeMemoryData oldValueData, boolean isNewPut, boolean disableDeferredDispose) {
        // Dispose old value if exist
        if (oldValueData != null) {
            cacheRecordProcessor.disposeData(oldValueData);
        }

        // If there is no new put, this means that key is not used.
        if (!isNewPut && key instanceof NativeMemoryData) {
            if (disableDeferredDispose) {
                // If deferred dispose is disabled, disabling unused key is our responsibility.
                // So dispose unused key at here.
                // But since it is allocated outside (it is already `NativeMemoryData`)
                // as not through `cacheRecordProcessor` but serialization service,
                // don't dispose it through `cacheRecordProcessor`.
                serializationService.disposeData(key);
            } else {
                // Add key to deferred list if it is `NativeMemoryData`, then operation will dispose it later
                cacheRecordProcessor.addDeferredDispose((NativeMemoryData) key);
            }
        }

        // If key is `NativeMemoryData`, since there is no convertion,
        // add its memory to used memory explicitly.
        if (isNewPut && key instanceof NativeMemoryData) {
            long size = cacheRecordProcessor.getSize((NativeMemoryData) key);
            cacheRecordProcessor.increaseUsedMemory(size);
        }

        // If value is `NativeMemoryData`, since there is no convertion,
        // add its memory to used memory explicitly.
        if (value instanceof NativeMemoryData) {
            long size = cacheRecordProcessor.getSize((NativeMemoryData) value);
            cacheRecordProcessor.increaseUsedMemory(size);
        }
    }

    protected void onOwnError(Data key, Object value, long ttlMillis, HiDensityNativeMemoryCacheRecord record,
                              NativeMemoryData oldValueData, boolean isNewPut,
                              boolean disableDeferredDispose, Throwable error) {
        // If record is created
        if (isNewPut && isMemoryBlockValid(record)) {
            if (value instanceof NativeMemoryData) {
                record.setValue(null);
                // If value is allocated outside of record store, disposing value is its responsibility.
                // So just dispose record which is allocated here.
                cacheRecordProcessor.free(record.address(), record.size());
                record.reset(NULL_PTR);
            } else {
                cacheRecordProcessor.dispose(record);
            }
        }
    }

    @Override
    public boolean putBackup(Data key, Object value, ExpiryPolicy expiryPolicy) {
        long ttl = expiryPolicyToTTL(expiryPolicy);
        return own(key, value, ttl, false);
    }

    private long expiryPolicyToTTL(ExpiryPolicy expiryPolicy) {
        if (expiryPolicy == null) {
            return CacheRecord.EXPIRATION_TIME_NOT_AVAILABLE;
        }
        try {
            Duration expiryDuration = expiryPolicy.getExpiryForCreation();
            if (expiryDuration == null || expiryDuration.isEternal()) {
                return CacheRecord.EXPIRATION_TIME_NOT_AVAILABLE;
            }
            long durationAmount = expiryDuration.getDurationAmount();
            TimeUnit durationTimeUnit = expiryDuration.getTimeUnit();
            return TimeUnit.MILLISECONDS.convert(durationAmount, durationTimeUnit);
        } catch (Exception e) {
            return CacheRecord.EXPIRATION_TIME_NOT_AVAILABLE;
        }
    }

    @Override
    public boolean putReplica(Data key, Object value, long ttlMillis) {
        return own(key, value, ttlMillis, true);
    }

    private boolean own(Data key, Object value, long ttlMillis, boolean disableDeferredDispose) {
        if (!records.containsKey(key)) {
            evictIfRequired();
        }

        long now = Clock.currentTimeMillis();
        long creationTime;
        HiDensityNativeMemoryCacheRecord record = null;
        NativeMemoryData oldValueData = null;
        boolean isNewPut = false;

        try {
            record = records.get(key);
            if (record == null) {
                isNewPut = true;
                record = createRecord(value, now);
                creationTime = now;
                records.put(key, record);
            } else {
                oldValueData = record.getValue();
                creationTime = record.getCreationTime();
                record.setValue(toNativeMemoryData(value));
            }

            onAccess(now, record, creationTime);

            ttlMillis = ttlMillis < Integer.MAX_VALUE ? ttlMillis : Integer.MAX_VALUE;
            record.setTtlMillis((int) ttlMillis);

            onOwn(key, value, ttlMillis, record, oldValueData, isNewPut, disableDeferredDispose);

            return isNewPut;
        } catch (Throwable error) {
            onOwnError(key, value, ttlMillis, record, oldValueData, isNewPut, disableDeferredDispose, error);

            throw ExceptionUtil.rethrow(error);
        }
    }
    //CHECKSTYLE:ON

    @Override
    protected void onPutIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy, String caller,
                                 boolean disableWriteThrough, HiDensityNativeMemoryCacheRecord record,
                                 boolean isExpired, boolean isSaveSucceed) {
        // If put is successful
        if (isSaveSucceed) {
            // If key is `NativeMemoryData`, since there is no convertion,
            // add its memory to used memory explicitly.
            if (key instanceof NativeMemoryData) {
                long size = cacheRecordProcessor.getSize((NativeMemoryData) key);
                cacheRecordProcessor.increaseUsedMemory(size);
            }
            // If value is `NativeMemoryData`, since there is no convertion,
            // add its memory to used memory explicitly.
            if (value instanceof NativeMemoryData) {
                long size = cacheRecordProcessor.getSize((NativeMemoryData) value);
                cacheRecordProcessor.increaseUsedMemory(size);
            }
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
        // Old value is disposed at `onUpdateRecord`

        // If replace is successful
        if (replaced) {
            // If value is `NativeMemoryData`, since there is no convertion,
            // add its memory to used memory explicitly.
            if (newValue instanceof NativeMemoryData) {
                long size = cacheRecordProcessor.getSize((NativeMemoryData) newValue);
                cacheRecordProcessor.increaseUsedMemory(size);
            }
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
    }

    @Override
    protected void onRemoveError(Data key, Object value, String caller, boolean getValue,
                                 HiDensityNativeMemoryCacheRecord record, boolean removed, Throwable error) {
        // If record has been somehow removed and if it is still valid, dispose it and its data
        if (removed && isMemoryBlockValid(record)) {
            cacheRecordProcessor.dispose(record);
            return;
        }
    }

    @Override
    public boolean merge(CacheEntryView<Data, Data> cacheEntryView, CacheMergePolicy mergePolicy,
                         String caller, int completionId, String origin) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        boolean merged = false;
        Data key = cacheEntryView.getKey();
        Object value = cacheEntryView.getValue();
        long expiryTime = cacheEntryView.getExpirationTime();
        HiDensityNativeMemoryCacheRecord record = records.get(key);
        boolean isExpired = processExpiredEntry(key, record, now);

        try {
            if (record == null || isExpired) {
                merged = createRecordWithExpiry(key, value, expiryTime,
                                                now, true, completionId, origin) != null;
            } else {
                NativeMemoryData existingValue = record.getValue();
                Object newValue =
                        mergePolicy.merge(name, 
                                          cacheEntryView,
                                          new DefaultCacheEntryView(key,
                                                                    existingValue,
                                                                    record.getExpirationTime(),
                                                                    record.getAccessTime(),
                                                                    record.getAccessHit()));
                if (existingValue != newValue) {
                    merged = updateRecordWithExpiry(key, newValue, record, expiryTime,
                                                    now, true, completionId, caller, origin);
                }
                publishEvent(createCacheCompleteEvent(toHeapData(key),
                                                      CacheRecord.EXPIRATION_TIME_NOT_AVAILABLE,
                                                      origin, completionId));
            }

            onMerge(cacheEntryView, mergePolicy, caller, true, record, isExpired, merged);

            if (merged && isStatisticsEnabled()) {
                statistics.increaseCachePuts(1);
                statistics.addPutTimeNanos(System.nanoTime() - start);
            }

            return merged;
        } catch (Throwable error) {
            onMergeError(cacheEntryView, mergePolicy, caller, true, record, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    protected void onMerge(CacheEntryView<Data, Data> cacheEntryView, CacheMergePolicy mergePolicy,
                           String caller, boolean disableWriteThrough, CacheRecord record,
                           boolean isExpired, boolean isSaveSucceed) {
    }

    protected void onMergeError(CacheEntryView<Data, Data> cacheEntryView, CacheMergePolicy mergePolicy,
                                String caller, boolean disableWriteThrough, CacheRecord record, Throwable error) {
    }

    protected void onEntryInvalidated(Data key, String source) {
        invalidateEntry(key, source);
    }

    @Override
    public int forceEvict() {
        int evictedCount = records.forceEvict(HiDensityCacheRecordStore.DEFAULT_FORCED_EVICTION_PERCENTAGE);
        if (isStatisticsEnabled() && evictedCount > 0) {
            statistics.increaseCacheEvictions(evictedCount);
        }
        return evictedCount;
    }

    @Override
    public void clear() {
        MemoryManager memoryManager = serializationService.getMemoryManager();
        if (memoryManager == null || memoryManager.isDestroyed()) {
            // otherwise will cause a SIGSEGV
            return;
        }
        super.clear();
    }

    @Override
    protected void onDestroy() {
        records.destroy();
    }

    @Override
    public void destroy() {
        MemoryManager memoryManager = serializationService.getMemoryManager();
        if (memoryManager == null || memoryManager.isDestroyed()) {
            // otherwise will cause a SIGSEGV
            return;
        }
        super.destroy();
    }

}
