package com.hazelcast.internal.nearcache.impl.nativememory;

import com.hazelcast.cache.hidensity.impl.nativememory.CacheHiDensityRecordProcessor;
import com.hazelcast.cache.hidensity.maxsize.HiDensityEntryCountMaxSizeChecker;
import com.hazelcast.cache.hidensity.maxsize.HiDensityFreeNativeMemoryPercentageMaxSizeChecker;
import com.hazelcast.cache.hidensity.maxsize.HiDensityFreeNativeMemorySizeMaxSizeChecker;
import com.hazelcast.cache.hidensity.maxsize.HiDensityUsedNativeMemoryPercentageMaxSizeChecker;
import com.hazelcast.cache.hidensity.maxsize.HiDensityUsedNativeMemorySizeMaxSizeChecker;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.eviction.ExpirationChecker;
import com.hazelcast.internal.eviction.MaxSizeChecker;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityRecordStore;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.nearcache.HiDensityNearCacheRecordStore;
import com.hazelcast.internal.nearcache.NearCacheRecord;
import com.hazelcast.internal.nearcache.impl.store.AbstractNearCacheRecordStore;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.util.Clock;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.internal.nearcache.NearCache.NULL_OBJECT;
import static com.hazelcast.internal.serialization.impl.NativeMemoryData.NATIVE_MEMORY_DATA_OVERHEAD;

/**
 * @param <K> the type of the key stored in Near Cache.
 * @param <V> the type of the value stored in Near Cache.
 */
public class HiDensityNativeMemoryNearCacheRecordStore<K, V>
        extends AbstractNearCacheRecordStore<K, V, Data, HiDensityNativeMemoryNearCacheRecord,
        HiDensityNativeMemoryNearCacheRecordMap>
        implements HiDensityNearCacheRecordStore<K, V, HiDensityNativeMemoryNearCacheRecord> {

    private static final int DEFAULT_INITIAL_CAPACITY = 256;

    private HazelcastMemoryManager memoryManager;
    private HiDensityNativeMemoryNearCacheRecordAccessor recordAccessor;
    private HiDensityStorageInfo storageInfo;
    private HiDensityRecordProcessor<HiDensityNativeMemoryNearCacheRecord> recordProcessor;
    private final RecordEvictionListener recordEvictionListener = new RecordEvictionListener();
    private final RecordExpirationChecker recordExpirationChecker = new RecordExpirationChecker();

    public HiDensityNativeMemoryNearCacheRecordStore(NearCacheConfig nearCacheConfig, EnterpriseSerializationService ss,
                                                     ClassLoader classLoader) {
        this(nearCacheConfig, new NearCacheStatsImpl(),
                new HiDensityStorageInfo(nearCacheConfig.getName()), ss, classLoader);
    }

    public HiDensityNativeMemoryNearCacheRecordStore(NearCacheConfig nearCacheConfig, NearCacheStatsImpl nearCacheStats,
                                                     HiDensityStorageInfo storageInfo, EnterpriseSerializationService ss,
                                                     ClassLoader classLoader) {
        super(nearCacheConfig, nearCacheStats, ss, classLoader);
        this.storageInfo = storageInfo;
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private void ensureInitialized(NearCacheConfig nearCacheConfig) {
        EnterpriseSerializationService serializationService = (EnterpriseSerializationService) super.serializationService;

        if (memoryManager == null) {
            HazelcastMemoryManager mm = serializationService.getMemoryManager();
            this.memoryManager = mm instanceof PoolingMemoryManager
                    ? ((PoolingMemoryManager) mm).getGlobalMemoryManager() : mm;
        }

        storageInfo = storageInfo == null ? new HiDensityStorageInfo(nearCacheConfig.getName()) : storageInfo;

        if (recordAccessor == null) {
            this.recordAccessor = new HiDensityNativeMemoryNearCacheRecordAccessor(serializationService, memoryManager);
        }

        if (recordProcessor == null) {
            this.recordProcessor = new CacheHiDensityRecordProcessor<HiDensityNativeMemoryNearCacheRecord>(
                    serializationService, recordAccessor, memoryManager, storageInfo);
        }
    }

    @Override
    protected HiDensityNativeMemoryNearCacheRecordMap createNearCacheRecordMap(NearCacheConfig nearCacheConfig) {
        ensureInitialized(nearCacheConfig);
        return new HiDensityNativeMemoryNearCacheRecordMap(DEFAULT_INITIAL_CAPACITY, recordProcessor, storageInfo);
    }

    @Override
    protected MaxSizeChecker createNearCacheMaxSizeChecker(EvictionConfig evictionConfig, NearCacheConfig nearCacheConfig) {
        ensureInitialized(nearCacheConfig);

        EvictionConfig.MaxSizePolicy maxSizePolicy = evictionConfig.getMaximumSizePolicy();
        if (maxSizePolicy == null) {
            throw new IllegalArgumentException("Max-size policy cannot be null");
        }

        int size = evictionConfig.getSize();
        long maxNativeMemory = ((EnterpriseSerializationService) super.serializationService)
                .getMemoryManager().getMemoryStats().getMaxNative();
        switch (maxSizePolicy) {
            case ENTRY_COUNT:
                return new HiDensityEntryCountMaxSizeChecker(storageInfo, size);
            case USED_NATIVE_MEMORY_SIZE:
                return new HiDensityUsedNativeMemorySizeMaxSizeChecker(storageInfo, size);
            case USED_NATIVE_MEMORY_PERCENTAGE:
                return new HiDensityUsedNativeMemoryPercentageMaxSizeChecker(storageInfo, size, maxNativeMemory);
            case FREE_NATIVE_MEMORY_SIZE:
                return new HiDensityFreeNativeMemorySizeMaxSizeChecker(memoryManager, size);
            case FREE_NATIVE_MEMORY_PERCENTAGE:
                return new HiDensityFreeNativeMemoryPercentageMaxSizeChecker(memoryManager, size, maxNativeMemory);
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

    HiDensityNativeMemoryNearCacheRecordMap getRecords() {
        return records;
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

    private HiDensityNativeMemoryNearCacheRecord createRecord(Object value, long creationTime, long expiryTime) {
        return createRecordInternal(value, creationTime, expiryTime, false, true);
    }

    private HiDensityNativeMemoryNearCacheRecord createRecordInternal(
            Object value, long creationTime, long expiryTime, boolean forceEvict, boolean retryOnOutOfMemoryError) {
        if (forceEvict) {
            forceEvict();
        }
        NativeMemoryData data = null;
        HiDensityNativeMemoryNearCacheRecord record;
        long recordAddress = NULL_ADDRESS;
        try {
            recordAddress = recordProcessor.allocate(HiDensityNativeMemoryNearCacheRecord.SIZE);
            record = recordProcessor.newRecord();
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
    protected long getKeyStorageMemoryCost(K key) {
        // because the key will be saved as native memory data with native memory data header
        // and this is not covered at "totalSize()" method
        return key instanceof Data ? NATIVE_MEMORY_DATA_OVERHEAD + ((Data) key).totalSize() : 0L;
    }

    @Override
    protected long getRecordStorageMemoryCost(HiDensityNativeMemoryNearCacheRecord record) {
        return record.size();
    }

    @Override
    protected HiDensityNativeMemoryNearCacheRecord valueToRecord(V value) {
        long creationTime = Clock.currentTimeMillis();
        return timeToLiveMillis > 0
                ? createRecord(value, creationTime, creationTime + timeToLiveMillis)
                : createRecord(value, creationTime, NearCacheRecord.TIME_NOT_SET);
    }

    @Override
    protected V recordToValue(HiDensityNativeMemoryNearCacheRecord record) {
        if (record.getValue() == null) {
            nearCacheStats.incrementMisses();
            return (V) NULL_OBJECT;
        }
        if (!isMemoryBlockValid(record)) {
            return null;
        }
        return (V) recordProcessor.readValue(record);
    }

    @Override
    public HiDensityNativeMemoryNearCacheRecord getRecord(K key) {
        return records.get(toData(key));
    }

    @Override
    protected HiDensityNativeMemoryNearCacheRecord putRecord(K key, HiDensityNativeMemoryNearCacheRecord record) {
        NativeMemoryData keyData = toNativeMemoryData(key);
        HiDensityNativeMemoryNearCacheRecord oldRecord = records.put(keyData, record);
        nearCacheStats.incrementOwnedEntryMemoryCost(getTotalStorageMemoryCost((K) keyData, record));
        return oldRecord;
    }

    @Override
    protected void putToRecord(HiDensityNativeMemoryNearCacheRecord record, V value) {
        NativeMemoryData oldValue = record.getValue();
        record.setValue(toNativeMemoryData(value));
        if (isMemoryBlockValid(oldValue)) {
            recordProcessor.disposeData(oldValue);
        }
    }

    @Override
    protected HiDensityNativeMemoryNearCacheRecord removeRecord(K key) {
        Data keyData = toData(key);
        HiDensityNativeMemoryNearCacheRecord removedRecord = records.remove(keyData);
        if (removedRecord != null) {
            nearCacheStats.decrementOwnedEntryMemoryCost(getTotalStorageMemoryCost((K) keyData, removedRecord));
        }
        return removedRecord;
    }

    @Override
    protected boolean containsRecordKey(K key) {
        Data keyData = toData(key);
        return records.containsKey(keyData);
    }

    @Override
    protected void onPut(K key, V value, HiDensityNativeMemoryNearCacheRecord record,
                         HiDensityNativeMemoryNearCacheRecord oldRecord) {
        // if old record is available, dispose it since it is replaced
        if (isMemoryBlockValid(oldRecord)) {
            recordProcessor.dispose(oldRecord);
        }
    }

    @Override
    protected void onPutError(K key, V value, HiDensityNativeMemoryNearCacheRecord record,
                              HiDensityNativeMemoryNearCacheRecord oldRecord, Throwable error) {
        // if old record is somehow allocated, dispose it since it is not in use
        if (isMemoryBlockValid(record)) {
            recordProcessor.dispose(record);
        }
    }

    @Override
    protected void onRemove(K key, HiDensityNativeMemoryNearCacheRecord record, boolean removed) {
        // if the record is available, dispose its data and put this to queue for reusing later
        if (record != null) {
            recordProcessor.dispose(record);
        }
    }

    @Override
    protected void onRemoveError(K key, HiDensityNativeMemoryNearCacheRecord record,
                                 boolean removed, Throwable error) {
        // if record has been somehow removed and if it is still valid, dispose it and its data
        if (removed && isMemoryBlockValid(record)) {
            recordProcessor.dispose(record);
        }
    }

    @Override
    public Object selectToSave(Object... candidates) {
        Object selectedCandidate = null;
        if (candidates != null && candidates.length > 0) {
            for (Object candidate : candidates) {
                // give priority to Data typed candidate
                // so there will be no extra conversion from Object to Data
                if (candidate instanceof Data) {
                    selectedCandidate = candidate;
                    break;
                }
            }
            if (selectedCandidate != null) {
                return selectedCandidate;
            } else {
                // select a non-null candidate
                for (Object candidate : candidates) {
                    if (candidate != null) {
                        selectedCandidate = candidate;
                        break;
                    }
                }
            }
        }
        return selectedCandidate;
    }

    @Override
    public int forceEvict() {
        checkAvailable();
        return records.forceEvict(HiDensityRecordStore.DEFAULT_FORCED_EVICTION_PERCENTAGE, recordEvictionListener);
    }

    @Override
    public void doExpiration() {
        checkAvailable();
        records.evictExpiredRecords(recordEvictionListener, recordExpirationChecker);
    }

    @Override
    public void loadKeys(DataStructureAdapter<Data, ?> adapter) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void storeKeys() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void onEvict(Data key, HiDensityNativeMemoryNearCacheRecord record, boolean wasExpired) {
        super.onEvict(key, record, wasExpired);
        nearCacheStats.decrementOwnedEntryMemoryCost(getTotalStorageMemoryCost((K) key, record));
    }

    @Override
    public HazelcastMemoryManager getMemoryManager() {
        return memoryManager;
    }

    /**
     * {@link EvictionListener} implementation for listening record eviction.
     */
    private class RecordEvictionListener implements EvictionListener<Data, HiDensityNativeMemoryNearCacheRecord> {

        @Override
        public void onEvict(Data key, HiDensityNativeMemoryNearCacheRecord record, boolean wasExpired) {
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
    private class RecordExpirationChecker implements ExpirationChecker<HiDensityNativeMemoryNearCacheRecord> {

        @Override
        public boolean isExpired(HiDensityNativeMemoryNearCacheRecord record) {
            return isRecordExpired(record);
        }
    }
}
