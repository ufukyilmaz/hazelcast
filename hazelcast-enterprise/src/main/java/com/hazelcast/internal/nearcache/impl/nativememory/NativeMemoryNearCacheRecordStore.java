package com.hazelcast.internal.nearcache.impl.nativememory;

import com.hazelcast.cache.impl.hidensity.nativememory.CacheHiDensityRecordProcessor;
import com.hazelcast.cache.impl.hidensity.maxsize.HiDensityEntryCountEvictionChecker;
import com.hazelcast.cache.impl.hidensity.maxsize.HiDensityFreeNativeMemoryPercentageEvictionChecker;
import com.hazelcast.cache.impl.hidensity.maxsize.HiDensityFreeNativeMemorySizeEvictionChecker;
import com.hazelcast.cache.impl.hidensity.maxsize.HiDensityUsedNativeMemoryPercentageEvictionChecker;
import com.hazelcast.cache.impl.hidensity.maxsize.HiDensityUsedNativeMemorySizeEvictionChecker;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.eviction.ExpirationChecker;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.nearcache.HiDensityNearCacheRecordStore;
import com.hazelcast.internal.nearcache.NearCacheRecord;
import com.hazelcast.internal.nearcache.impl.store.AbstractNearCacheRecordStore;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.util.Clock;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.internal.nearcache.NearCache.CACHED_AS_NULL;
import static com.hazelcast.internal.nearcache.NearCacheRecord.READ_PERMITTED;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static java.lang.Integer.getInteger;

/**
 * {@link com.hazelcast.internal.nearcache.NearCacheRecordStore} implementation for Near Caches
 * with {@link com.hazelcast.config.InMemoryFormat#NATIVE} in-memory-format.
 *
 * @param <K> the type of the key stored in Near Cache
 * @param <V> the type of the value stored in Near Cache
 */
@SuppressWarnings("checkstyle:methodcount")
public class NativeMemoryNearCacheRecordStore<K, V>
        extends AbstractNearCacheRecordStore<K, V, Data, NativeMemoryNearCacheRecord, NativeMemoryNearCacheRecordMap>
        implements HiDensityNearCacheRecordStore<K, V, NativeMemoryNearCacheRecord> {

    /**
     * Background expiration task can only scan at most this number of
     * entries in a round. This scanning is done under lock and has
     * potential to affect other operations if it takes too long time.
     */
    private static final int DEFAULT_MAX_SCANNABLE_ENTRY_COUNT_PER_LOOP = 100;
    private static final String PROP_MAX_SCANNABLE_ENTRY_COUNT_PER_LOOP
            = "hazelcast.internal.hd.near.cache.max.scannable.entry.count.per.loop";
    private static final int DEFAULT_INITIAL_CAPACITY = 256;

    /**
     * See {@link com.hazelcast.internal.elastic.map.BehmSlotAccessor#SLOT_LENGTH}
     */
    private static final int SLOT_COST_IN_BYTES = 16;

    private int maxScannableEntryCountPerLoop;

    private HiDensityStorageInfo storageInfo;
    private HazelcastMemoryManager memoryManager;
    private NativeMemoryNearCacheRecordAccessor recordAccessor;
    private HiDensityRecordProcessor<NativeMemoryNearCacheRecord> recordProcessor;
    private final RecordEvictionListener recordEvictionListener = new RecordEvictionListener();
    private final RecordExpirationChecker recordExpirationChecker = new RecordExpirationChecker();

    public NativeMemoryNearCacheRecordStore(NearCacheConfig nearCacheConfig, EnterpriseSerializationService ss,
                                            ClassLoader classLoader) {
        this(nearCacheConfig, new NearCacheStatsImpl(), new HiDensityStorageInfo(nearCacheConfig.getName()), ss, classLoader);
    }

    public NativeMemoryNearCacheRecordStore(NearCacheConfig nearCacheConfig, NearCacheStatsImpl nearCacheStats,
                                            HiDensityStorageInfo storageInfo, EnterpriseSerializationService ss,
                                            ClassLoader classLoader) {
        super(nearCacheConfig, nearCacheStats, ss, classLoader);
        this.storageInfo = storageInfo;
        this.maxScannableEntryCountPerLoop = getInteger(PROP_MAX_SCANNABLE_ENTRY_COUNT_PER_LOOP,
                DEFAULT_MAX_SCANNABLE_ENTRY_COUNT_PER_LOOP);
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
            this.recordAccessor = new NativeMemoryNearCacheRecordAccessor(serializationService, memoryManager);
        }

        if (recordProcessor == null) {
            this.recordProcessor = new CacheHiDensityRecordProcessor<>(serializationService,
                    recordAccessor, memoryManager, storageInfo);
        }
    }

    @Override
    protected NativeMemoryNearCacheRecordMap createNearCacheRecordMap(NearCacheConfig nearCacheConfig) {
        ensureInitialized(nearCacheConfig);
        return new NativeMemoryNearCacheRecordMap(DEFAULT_INITIAL_CAPACITY, recordProcessor, storageInfo);
    }

    @Override
    protected EvictionChecker createNearCacheEvictionChecker(EvictionConfig evictionConfig, NearCacheConfig nearCacheConfig) {
        ensureInitialized(nearCacheConfig);

        EvictionConfig.MaxSizePolicy maxSizePolicy = evictionConfig.getMaximumSizePolicy();
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
                        + EvictionConfig.MaxSizePolicy.ENTRY_COUNT + ", "
                        + EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE + ", "
                        + EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE + ", "
                        + EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE + ", "
                        + EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE
                        + " are supported.");
        }
    }

    private NativeMemoryData toNativeMemoryData(Object data) {
        NativeMemoryData nativeMemoryData;
        if (!(data instanceof Data)) {
            nativeMemoryData = (NativeMemoryData) recordProcessor.toData(data, DataType.NATIVE);
        } else if (!(data instanceof NativeMemoryData)) {
            nativeMemoryData = (NativeMemoryData) recordProcessor.convertData((Data) data, DataType.NATIVE);
        } else {
            nativeMemoryData = (NativeMemoryData) data;
        }
        return nativeMemoryData;
    }

    private static boolean isMemoryBlockValid(MemoryBlock memoryBlock) {
        return memoryBlock != null && memoryBlock.address() != NULL_ADDRESS;
    }

    private NativeMemoryNearCacheRecord createRecord(Object value, long creationTime, long expiryTime) {
        return createRecordInternal(value, creationTime, expiryTime, false, true);
    }

    private NativeMemoryNearCacheRecord createRecordInternal(Object value, long creationTime, long expiryTime,
                                                             boolean forceEvict, boolean retryOnOutOfMemoryError) {
        if (forceEvict) {
            forceEvict();
        }
        NativeMemoryData data = null;
        NativeMemoryNearCacheRecord record;
        long recordAddress = NULL_ADDRESS;
        try {
            recordAddress = recordProcessor.allocate(NativeMemoryNearCacheRecord.SIZE);
            record = recordProcessor.newRecord();
            record.reset(recordAddress);
            record.casRecordState(0, READ_PERMITTED);

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
                return createRecordInternal(value, creationTime, expiryTime, true, false);
            } else {
                throw e;
            }
        }
    }

    @Override
    public void invalidate(K key) {
        checkAvailable();

        NativeMemoryNearCacheRecord record = null;
        boolean removed = false;
        try {
            record = removeRecord(key);
            if (canUpdateStats(record)) {
                removed = true;
                nearCacheStats.decrementOwnedEntryCount();
                nearCacheStats.incrementInvalidations();
            }
            onRemove(key, record, removed);
        } catch (Throwable error) {
            onRemoveError(key, record, removed, error);
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
    protected long getRecordStorageMemoryCost(NativeMemoryNearCacheRecord record) {
        long slotValueCost = SLOT_COST_IN_BYTES / 2;
        return slotValueCost + recordAccessor.getSize(record) + recordAccessor.getSize(record.getValue());
    }

    @Override
    protected NativeMemoryNearCacheRecord createRecord(V value) {
        long creationTime = Clock.currentTimeMillis();
        return timeToLiveMillis > 0
                ? createRecord(value, creationTime, creationTime + timeToLiveMillis)
                : createRecord(value, creationTime, NearCacheRecord.TIME_NOT_SET);
    }

    @Override
    protected void updateRecordValue(NativeMemoryNearCacheRecord record, V value) {
        NativeMemoryData nativeValue = null;
        try {
            nativeValue = toNativeMemoryData(value);
            record.setValue(nativeValue);
        } catch (Throwable throwable) {
            if (isMemoryBlockValid(nativeValue)) {
                recordProcessor.disposeData(nativeValue);
            }
            throw rethrow(throwable);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected V recordToValue(NativeMemoryNearCacheRecord record) {
        if (record.getValue() == null) {
            return (V) CACHED_AS_NULL;
        }
        if (!isMemoryBlockValid(record)) {
            return null;
        }
        return (V) recordProcessor.readValue(record);
    }

    @Override
    public NativeMemoryNearCacheRecord getRecord(K key) {
        return records.get(toData(key));
    }

    @Override
    protected NativeMemoryNearCacheRecord getOrCreateToReserve(K key, Data keyData) {
        NativeMemoryNearCacheRecord recordToReserve = getRecord(key);
        if (recordToReserve != null) {
            return recordToReserve;
        }

        NativeMemoryNearCacheRecord record;
        NativeMemoryData nativeKey = null;
        try {
            record = new ReserveForUpdateFunction(keyData).apply(key);
            nativeKey = toNativeMemoryData(key);
            records.put(nativeKey, record);
        } catch (Throwable throwable) {
            if (isMemoryBlockValid(nativeKey)) {
                recordProcessor.disposeData(nativeKey);
            }
            throw rethrow(throwable);
        }
        return record;
    }

    @Override
    protected V updateAndGetReserved(K key, V value, long reservationId, boolean deserialize) {
        NativeMemoryNearCacheRecord reservedRecord = getRecord(key);
        if (reservedRecord == null) {
            return null;
        }

        NativeMemoryNearCacheRecord record = updateReservedRecordInternal(key, value, reservedRecord, reservationId);
        if (!deserialize) {
            return null;
        }

        return toValue(record.getValue());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected NativeMemoryNearCacheRecord putRecord(K key, NativeMemoryNearCacheRecord record) {
        assert key != null;

        NativeMemoryData keyData = toNativeMemoryData(key);
        NativeMemoryNearCacheRecord oldRecord = records.put(keyData, record);
        nearCacheStats.incrementOwnedEntryMemoryCost(getTotalStorageMemoryCost((K) keyData, record));
        if (oldRecord != null) {
            nearCacheStats.decrementOwnedEntryMemoryCost(getTotalStorageMemoryCost((K) keyData, oldRecord));
        }
        return oldRecord;
    }

    private NativeMemoryNearCacheRecord removeRecord(K key) {
        Data keyData = toData(key);
        NativeMemoryData nativeKeyData = new NativeMemoryData();
        nativeKeyData.reset(records.getNativeKeyAddress(keyData));

        NativeMemoryNearCacheRecord removedRecord = records.remove(keyData);
        if (removedRecord != null) {
            nearCacheStats.decrementOwnedEntryMemoryCost(getTotalStorageMemoryCost((K) nativeKeyData, removedRecord));
        }
        return removedRecord;
    }

    @Override
    protected boolean containsRecordKey(K key) {
        Data keyData = toData(key);
        return records.containsKey(keyData);
    }

    @Override
    protected void onPut(K key, V value, NativeMemoryNearCacheRecord record, NativeMemoryNearCacheRecord oldRecord) {
        // if old record is available, dispose it since it is replaced
        if (isMemoryBlockValid(oldRecord)) {
            recordProcessor.dispose(oldRecord);
        }
    }

    @Override
    protected void onPutError(K key, V value, NativeMemoryNearCacheRecord record, NativeMemoryNearCacheRecord oldRecord,
                              Throwable error) {
        // if old record is somehow allocated, dispose it since it is not in use
        if (isMemoryBlockValid(record)) {
            recordProcessor.dispose(record);
        }
    }

    @Override
    protected void onRemove(K key, NativeMemoryNearCacheRecord record, boolean removed) {
        // if the record is available, dispose its data and put this to queue for reusing later
        if (record != null) {
            recordProcessor.dispose(record);
        }
    }

    @Override
    protected void onRemoveError(K key, NativeMemoryNearCacheRecord record, boolean removed, Throwable error) {
        // if record has been somehow removed and if it's still valid, dispose it and its data
        if (removed && isMemoryBlockValid(record)) {
            recordProcessor.dispose(record);
        }
    }

    @Override
    public int forceEvict() {
        checkAvailable();
        return records.forceEvict(DEFAULT_FORCED_EVICTION_PERCENTAGE, recordEvictionListener);
    }

    @Override
    public void doExpiration() {
        checkAvailable();
        records.scanByNumberToDeleteExpired(recordEvictionListener,
                recordExpirationChecker, maxScannableEntryCountPerLoop);
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
    public void onEvict(Data key, NativeMemoryNearCacheRecord record, boolean wasExpired) {
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
     * {@link EvictionListener} implementation for listening record eviction.
     */
    private class RecordEvictionListener implements EvictionListener<Data, NativeMemoryNearCacheRecord> {

        @Override
        @SuppressWarnings("unchecked")
        public void onEvict(Data key, NativeMemoryNearCacheRecord record, boolean wasExpired) {
            if (wasExpired) {
                nearCacheStats.incrementExpirations();
            } else {
                nearCacheStats.incrementEvictions();
            }
            nearCacheStats.decrementOwnedEntryCount();
            nearCacheStats.decrementOwnedEntryMemoryCost(getTotalStorageMemoryCost((K) key, record));
        }
    }

    /**
     * {@link ExpirationChecker} implementation for checking record expiration.
     */
    private class RecordExpirationChecker implements ExpirationChecker<NativeMemoryNearCacheRecord> {

        @Override
        public boolean isExpired(NativeMemoryNearCacheRecord record) {
            return isRecordExpired(record);
        }
    }
}
