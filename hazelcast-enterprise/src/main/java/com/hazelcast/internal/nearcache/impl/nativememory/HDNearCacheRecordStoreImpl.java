package com.hazelcast.internal.nearcache.impl.nativememory;

import com.hazelcast.cache.impl.hidensity.maxsize.HiDensityEntryCountEvictionChecker;
import com.hazelcast.cache.impl.hidensity.maxsize.HiDensityFreeNativeMemoryPercentageEvictionChecker;
import com.hazelcast.cache.impl.hidensity.maxsize.HiDensityFreeNativeMemorySizeEvictionChecker;
import com.hazelcast.cache.impl.hidensity.maxsize.HiDensityUsedNativeMemoryPercentageEvictionChecker;
import com.hazelcast.cache.impl.hidensity.maxsize.HiDensityUsedNativeMemorySizeEvictionChecker;
import com.hazelcast.cache.impl.hidensity.nativememory.CacheHiDensityRecordProcessor;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.eviction.ExpirationChecker;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.internal.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.internal.nearcache.HDNearCacheRecordStore;
import com.hazelcast.internal.nearcache.impl.store.AbstractNearCacheRecordStore;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.memory.NativeOutOfMemoryError;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.internal.nearcache.NearCache.CACHED_AS_NULL;
import static com.hazelcast.internal.nearcache.NearCacheRecord.READ_PERMITTED;
import static com.hazelcast.internal.nearcache.NearCacheRecord.TIME_NOT_SET;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * {@link com.hazelcast.internal.nearcache.NearCacheRecordStore}
 * implementation for Near Caches with {@link
 * com.hazelcast.config.InMemoryFormat#NATIVE} in-memory-format.
 *
 * @param <K> the type of the key stored in Near Cache
 * @param <V> the type of the value stored in Near Cache
 */
@SuppressWarnings("checkstyle:methodcount")
public class HDNearCacheRecordStoreImpl<K, V>
        extends AbstractNearCacheRecordStore<K, V, Data, HDNearCacheRecord, HDNearCacheRecordMap>
        implements HDNearCacheRecordStore<K, V, HDNearCacheRecord> {

    private static final int DEFAULT_INITIAL_CAPACITY = 256;

    /**
     * See sloth length in {@link
     * com.hazelcast.internal.elastic.map.BehmSlotAccessor}
     */
    private static final int SLOT_COST_IN_BYTES = 16;


    private HiDensityStorageInfo storageInfo;
    private HazelcastMemoryManager memoryManager;
    private HDNearCacheRecordAccessor recordAccessor;
    private HiDensityRecordProcessor<HDNearCacheRecord> recordProcessor;

    private final int sampleCount;
    private final RecordExpirationChecker recordExpirationChecker = new RecordExpirationChecker();

    public HDNearCacheRecordStoreImpl(NearCacheConfig nearCacheConfig, EnterpriseSerializationService ss,
                                      ClassLoader classLoader, int sampleCount) {
        this(nearCacheConfig, new NearCacheStatsImpl(),
                new HiDensityStorageInfo(nearCacheConfig.getName()), ss, classLoader, sampleCount);
    }

    public HDNearCacheRecordStoreImpl(NearCacheConfig nearCacheConfig, NearCacheStatsImpl nearCacheStats,
                                      HiDensityStorageInfo storageInfo, EnterpriseSerializationService ss,
                                      ClassLoader classLoader, int sampleCount) {
        super(nearCacheConfig, nearCacheStats, ss, classLoader);
        this.storageInfo = storageInfo;
        this.sampleCount = sampleCount;
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private void ensureInitialized(NearCacheConfig nearCacheConfig) {
        EnterpriseSerializationService serializationService = (EnterpriseSerializationService) super.serializationService;

        if (memoryManager == null) {
            HazelcastMemoryManager mm = serializationService.getMemoryManager();
            this.memoryManager = mm instanceof PoolingMemoryManager ? ((PoolingMemoryManager) mm).getGlobalMemoryManager() : mm;
        }

        storageInfo = storageInfo == null ? new HiDensityStorageInfo(nearCacheConfig.getName()) : storageInfo;

        if (recordAccessor == null) {
            this.recordAccessor = new HDNearCacheRecordAccessor(serializationService, memoryManager);
        }

        if (recordProcessor == null) {
            this.recordProcessor = new CacheHiDensityRecordProcessor<>(serializationService,
                    recordAccessor, memoryManager, storageInfo);
        }
    }

    @Override
    protected HDNearCacheRecordMap createNearCacheRecordMap(NearCacheConfig nearCacheConfig) {
        ensureInitialized(nearCacheConfig);
        return new HDNearCacheRecordMap(DEFAULT_INITIAL_CAPACITY, recordProcessor, storageInfo);
    }

    @Override
    protected EvictionChecker createNearCacheEvictionChecker(EvictionConfig evictionConfig, NearCacheConfig nearCacheConfig) {
        ensureInitialized(nearCacheConfig);

        MaxSizePolicy maxSizePolicy = evictionConfig.getMaxSizePolicy();
        if (maxSizePolicy == null) {
            throw new IllegalArgumentException("Max-size policy cannot be null");
        }

        int size = evictionConfig.getSize();
        long maxNativeMemory = ((EnterpriseSerializationService) serializationService).getMemoryManager()
                .getMemoryStats().getMaxNative();
        switch (maxSizePolicy) {
            case ENTRY_COUNT:
                return new HiDensityEntryCountEvictionChecker(storageInfo, size);
            case USED_NATIVE_MEMORY_SIZE:
                return new HiDensityUsedNativeMemorySizeEvictionChecker(storageInfo, size);
            case USED_NATIVE_MEMORY_PERCENTAGE:
                return new HiDensityUsedNativeMemoryPercentageEvictionChecker(storageInfo, size, maxNativeMemory);
            case FREE_NATIVE_MEMORY_SIZE:
                return new HiDensityFreeNativeMemorySizeEvictionChecker(memoryManager, size);
            case FREE_NATIVE_MEMORY_PERCENTAGE:
                return new HiDensityFreeNativeMemoryPercentageEvictionChecker(memoryManager, size, maxNativeMemory);
            default:
                throw new IllegalArgumentException("Invalid max-size policy "
                        + '(' + maxSizePolicy + ") for " + getClass().getName() + "! Only "
                        + MaxSizePolicy.ENTRY_COUNT + ", "
                        + MaxSizePolicy.USED_NATIVE_MEMORY_SIZE + ", "
                        + MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE + ", "
                        + MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE + ", "
                        + MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE
                        + " are supported.");
        }
    }

    private NativeMemoryData toNativeMemoryData(Object data) {
        if (!(data instanceof Data)) {
            return (NativeMemoryData) recordProcessor.toData(data, DataType.NATIVE);
        }

        if (!(data instanceof NativeMemoryData)) {
            return (NativeMemoryData) recordProcessor.convertData((Data) data, DataType.NATIVE);
        }

        return (NativeMemoryData) data;
    }

    private static boolean isMemoryBlockValid(MemoryBlock memoryBlock) {
        return memoryBlock != null && memoryBlock.address() != NULL_ADDRESS;
    }

    private HDNearCacheRecord createRecord(Object value, long creationTime, long expiryTime) {
        return createRecordInternal(value, creationTime, expiryTime, false, true);
    }

    private HDNearCacheRecord createRecordInternal(Object value, long creationTime, long expirationTime,
                                                   boolean forceEvict, boolean retryOnOutOfMemoryError) {
        if (forceEvict) {
            forceEvict();
        }

        NativeMemoryData data = null;
        HDNearCacheRecord record;
        long recordAddress = NULL_ADDRESS;
        try {
            recordAddress = recordProcessor.allocate(HDNearCacheRecord.SIZE);
            record = recordProcessor.newRecord();
            record.reset(recordAddress);
            record.setReservationId(READ_PERMITTED);
            record.setCreationTime(creationTime);
            record.setExpirationTime(expirationTime);
            record.setLastAccessTime(TIME_NOT_SET);

            if (value != null) {
                data = toNativeMemoryData(value);
                record.setValueAddress(data.address());
            } else {
                record.setValueAddress(NULL_ADDRESS);
            }
            return record;
        } catch (NativeOutOfMemoryError e) {
            // if any memory region is allocated for record, dispose it
            if (recordAddress != NULL_ADDRESS) {
                recordProcessor.dispose(recordAddress);
            }
            // if any data is allocated for record, dispose it
            if (isMemoryBlockValid(data)) {
                recordProcessor.disposeData(data);
            }
            if (retryOnOutOfMemoryError) {
                return createRecordInternal(value, creationTime, expirationTime, true, false);
            } else {
                throw e;
            }
        }
    }

    @Override
    public void invalidate(K key) {
        checkAvailable();

        HDNearCacheRecord record = null;
        try {
            Data keyData = toData(key);
            NativeMemoryData nativeKeyData
                    = new NativeMemoryData().reset(records.getNativeKeyAddress(keyData));

            record = records.remove(keyData);

            if (canUpdateStats(record)) {
                nearCacheStats.decrementOwnedEntryCount();
                nearCacheStats.incrementInvalidations();
                nearCacheStats.decrementOwnedEntryMemoryCost(getTotalStorageMemoryCost((K) nativeKeyData, record));
            }

            onRemove(key, record, record != null);
        } catch (Throwable error) {
            onRemoveError(key, record, record != null, error);
            throw rethrow(error);
        } finally {
            nearCacheStats.incrementInvalidationRequests();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected long getKeyStorageMemoryCost(K key) {
        if (key instanceof Data && !(key instanceof NativeMemoryData)) {
            NativeMemoryData nativeKeyData = new NativeMemoryData();
            nativeKeyData.reset(records.getNativeKeyAddress((Data) key));
            return getKeyStorageMemoryCost((K) nativeKeyData);
        }

        long slotKeyCost = SLOT_COST_IN_BYTES / 2;
        return key instanceof MemoryBlock ? slotKeyCost + recordAccessor.getSize((MemoryBlock) key) : 0L;
    }

    @Override
    protected long getRecordStorageMemoryCost(HDNearCacheRecord record) {
        long slotValueCost = SLOT_COST_IN_BYTES / 2;
        return slotValueCost + recordAccessor.getSize(record) + recordAccessor.getSize(record.getValue());
    }

    @Override
    protected HDNearCacheRecord createRecord(V value) {
        long creationTime = Clock.currentTimeMillis();
        return timeToLiveMillis > 0
                ? createRecord(value, creationTime, creationTime + timeToLiveMillis)
                : createRecord(value, creationTime, TIME_NOT_SET);
    }

    @Override
    protected void updateRecordValue(HDNearCacheRecord record, V value) {
        NativeMemoryData newValue = null;
        try {
            NativeMemoryData oldValue = record.getValue();

            newValue = toNativeMemoryData(value);
            record.setValue(newValue);

            if (isMemoryBlockValid(oldValue)) {
                recordProcessor.disposeData(oldValue);
            }

        } catch (Throwable throwable) {
            if (isMemoryBlockValid(newValue)) {
                recordProcessor.disposeData(newValue);
            }
            throw rethrow(throwable);
        }
    }

    @Override
    protected HDNearCacheRecord reserveForCacheOnUpdate(K key, Data keyData, long reservationId) {
        HDNearCacheRecord existingRecord = records.get(keyData);
        HDNearCacheRecord reservedRecord = reserveForCacheOnUpdate0(key, keyData,
                existingRecord, reservationId);

        if (reservedRecord != null) {
            Data nativeKey = null;
            try {
                // if we have an existingRecord, it means we previously
                // created a key in HD memory and now we don't need
                // to create once again. Otherwise, when there is no
                // existingRecord, we have to create a new key in
                // HD memory for the newly created reservedRecord.
                nativeKey = existingRecord != null ? keyData : toNativeMemoryData(keyData);
                records.put(nativeKey, reservedRecord);
            } catch (Throwable throwable) {
                freeHDMemory(nativeKey, reservedRecord);
                throw rethrow(throwable);
            }
        } else {
            invalidate((K) keyData);
        }
        return reservedRecord;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected V recordToValue(HDNearCacheRecord record) {
        if (record.getValue() == null) {
            return (V) CACHED_AS_NULL;
        }
        if (!isMemoryBlockValid(record)) {
            return null;
        }
        return (V) recordProcessor.readValue(record);
    }

    @Override
    public HDNearCacheRecord getRecord(K key) {
        return records.get(toData(key));
    }

    @Override
    protected HDNearCacheRecord getOrCreateToReserve(K key, Data keyData, long reservationId) {
        HDNearCacheRecord recordToReserve = getRecord(key);
        if (recordToReserve != null) {
            return recordToReserve;
        }

        HDNearCacheRecord record = null;
        NativeMemoryData nativeKey = null;
        try {
            record = reserveForUpdate0(key, keyData, reservationId);
            nativeKey = toNativeMemoryData(key);
            records.put(nativeKey, record);
        } catch (Throwable throwable) {
            freeHDMemory(nativeKey, record);

            throw rethrow(throwable);
        }
        return record;
    }

    private void freeHDMemory(Data key, HDNearCacheRecord record) {
        if (key instanceof NativeMemoryData
                && isMemoryBlockValid(((NativeMemoryData) key))) {
            recordProcessor.disposeData(key);
        }

        if (isMemoryBlockValid(record)) {
            recordProcessor.dispose(record);
        }
    }

    @Override
    protected V updateAndGetReserved(K key, V value, long reservationId, boolean deserialize) {
        HDNearCacheRecord reservedRecord = getRecord(key);
        if (reservedRecord == null) {
            return null;
        }

        HDNearCacheRecord record = updateReservedRecordInternal(key, value, reservedRecord, reservationId);
        if (!deserialize) {
            return null;
        }

        return toValue(record.getValue());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected HDNearCacheRecord putRecord(K key, HDNearCacheRecord record) {
        assert key != null;

        NativeMemoryData keyData = toNativeMemoryData(key);
        HDNearCacheRecord oldRecord = records.put(keyData, record);
        nearCacheStats.incrementOwnedEntryMemoryCost(getTotalStorageMemoryCost((K) keyData, record));
        if (oldRecord != null) {
            nearCacheStats.decrementOwnedEntryMemoryCost(getTotalStorageMemoryCost((K) keyData, oldRecord));
        }
        return oldRecord;
    }

    @Override
    protected boolean containsRecordKey(K key) {
        Data keyData = toData(key);
        return records.containsKey(keyData);
    }

    @Override
    protected void onPut(K key, V value, HDNearCacheRecord record, HDNearCacheRecord oldRecord) {
        // if old record is available, dispose it since it is replaced
        if (isMemoryBlockValid(oldRecord)) {
            recordProcessor.dispose(oldRecord);
        }
    }

    @Override
    protected void onPutError(K key, V value, HDNearCacheRecord record, HDNearCacheRecord oldRecord,
                              Throwable error) {
        // if old record is somehow allocated, dispose it since it is not in use
        if (isMemoryBlockValid(record)) {
            recordProcessor.dispose(record);
        }
    }

    @Override
    protected void onRemove(K key, HDNearCacheRecord record, boolean removed) {
        // if the record is available, dispose its data and put this to queue for reusing later
        if (record != null) {
            recordProcessor.dispose(record);
        }
    }

    @Override
    protected void onRemoveError(K key, HDNearCacheRecord record, boolean removed, Throwable error) {
        // if record has been somehow removed and if it's still valid, dispose it and its data
        if (removed && isMemoryBlockValid(record)) {
            recordProcessor.dispose(record);
        }
    }

    @Override
    public int forceEvict() {
        checkAvailable();
        return records.forceEvict(DEFAULT_FORCED_EVICTION_PERCENTAGE, this);
    }

    @Override
    public void doExpiration() {
        checkAvailable();
        records.sampleAndDeleteExpired(this,
                recordExpirationChecker, sampleCount);
    }

    @Override
    public void loadKeys(DataStructureAdapter<Object, ?> adapter) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void storeKeys() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onEvict(Data key, HDNearCacheRecord record, boolean wasExpired) {
        super.onEvict(key, record, wasExpired);
        nearCacheStats.decrementOwnedEntryMemoryCost(getTotalStorageMemoryCost((K) key, record));
    }

    @Override
    public HazelcastMemoryManager getMemoryManager() {
        return memoryManager;
    }

    @Override
    public void destroy() {
        checkAvailable();

        try {
            super.destroy();
        } finally {
            records.dispose();
            records = null;
        }
    }

    /**
     * {@link ExpirationChecker} implementation for checking record expiration.
     */
    private class RecordExpirationChecker implements ExpirationChecker<HDNearCacheRecord> {

        @Override
        public boolean isExpired(HDNearCacheRecord record) {
            return isRecordExpired(record);
        }
    }
}
