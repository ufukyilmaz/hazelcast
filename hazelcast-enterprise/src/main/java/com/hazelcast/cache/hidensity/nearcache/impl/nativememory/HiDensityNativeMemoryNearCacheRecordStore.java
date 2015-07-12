package com.hazelcast.cache.hidensity.nearcache.impl.nativememory;

import com.hazelcast.cache.hidensity.maxsize.HiDensityFreeNativeMemoryPercentageMaxSizeChecker;
import com.hazelcast.cache.hidensity.maxsize.HiDensityFreeNativeMemorySizeMaxSizeChecker;
import com.hazelcast.cache.hidensity.maxsize.HiDensityUsedNativeMemoryPercentageMaxSizeChecker;
import com.hazelcast.cache.hidensity.maxsize.HiDensityUsedNativeMemorySizeMaxSizeChecker;
import com.hazelcast.cache.hidensity.nearcache.HiDensityNearCacheContext;
import com.hazelcast.cache.hidensity.nearcache.HiDensityNearCacheRecordStore;
import com.hazelcast.cache.impl.eviction.EvictionListener;
import com.hazelcast.cache.impl.eviction.ExpirationChecker;
import com.hazelcast.cache.impl.maxsize.MaxSizeChecker;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheRecord;
import com.hazelcast.cache.impl.nearcache.impl.store.AbstractNearCacheRecordStore;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.hidensity.HiDensityRecordProcessor;
import com.hazelcast.hidensity.HiDensityRecordStore;
import com.hazelcast.hidensity.HiDensityStorageInfo;
import com.hazelcast.hidensity.impl.DefaultHiDensityRecordProcessor;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.impl.NativeMemoryData;
import com.hazelcast.util.Clock;

/**
 * @author sozal 26/10/14
 *
 * @param <K> the type of the key stored in near-cache
 * @param <V> the type of the value stored in near-cache
 */
public class HiDensityNativeMemoryNearCacheRecordStore<K, V>
        extends AbstractNearCacheRecordStore<K, V, Data, HiDensityNativeMemoryNearCacheRecord,
                                             HiDensityNativeMemoryNearCacheRecordMap>
        implements HiDensityNearCacheRecordStore<K, V, HiDensityNativeMemoryNearCacheRecord> {

    private static final int DEFAULT_INITIAL_CAPACITY = 256;

    private MemoryManager memoryManager;
    private HiDensityNativeMemoryNearCacheRecordAccessor recordAccessor;
    private HiDensityStorageInfo storageInfo;
    private HiDensityRecordProcessor<HiDensityNativeMemoryNearCacheRecord> recordProcessor;
    private final RecordEvictionListener recordEvictionListener = new RecordEvictionListener();
    private final RecordExpirationChecker recordExpirationChecker = new RecordExpirationChecker();

    public HiDensityNativeMemoryNearCacheRecordStore(NearCacheConfig nearCacheConfig,
                                                     NearCacheContext nearCacheContext) {
        this(nearCacheConfig,
             nearCacheContext,
             new NearCacheStatsImpl(),
             new HiDensityStorageInfo(nearCacheConfig.getName()));
    }


    public HiDensityNativeMemoryNearCacheRecordStore(NearCacheConfig nearCacheConfig,
                                                     NearCacheContext nearCacheContext,
                                                     NearCacheStatsImpl nearCacheStats,
                                                     HiDensityStorageInfo storageInfo) {
        super(nearCacheConfig,
              new HiDensityNearCacheContext(nearCacheContext, storageInfo),
              nearCacheStats);
    }

    private void ensureInitialized(NearCacheConfig nearCacheConfig,
                                   NearCacheContext nearCacheContext) {
        EnterpriseSerializationService serializationService =
                (EnterpriseSerializationService) nearCacheContext.getSerializationService();

        if (memoryManager == null) {
            MemoryManager mm = serializationService.getMemoryManager();
            if (mm instanceof PoolingMemoryManager) {
                this.memoryManager = ((PoolingMemoryManager) mm).getGlobalMemoryManager();
            } else {
                this.memoryManager = mm;
            }
        }

        if (storageInfo == null) {
            if (nearCacheContext instanceof HiDensityNearCacheContext) {
                storageInfo = ((HiDensityNearCacheContext) nearCacheContext).getStorageInfo();
            }
            if (storageInfo == null) {
                storageInfo = new HiDensityStorageInfo(nearCacheConfig.getName());
            }
        }

        if (recordAccessor == null) {
            this.recordAccessor =
                    new HiDensityNativeMemoryNearCacheRecordAccessor(serializationService, memoryManager);
        }

        if (recordProcessor == null) {
            this.recordProcessor =
                    new DefaultHiDensityRecordProcessor<HiDensityNativeMemoryNearCacheRecord>(
                            serializationService, recordAccessor,
                            memoryManager, storageInfo);
        }
    }

    @Override
    protected HiDensityNativeMemoryNearCacheRecordMap createNearCacheRecordMap(NearCacheConfig nearCacheConfig,
                                                                               NearCacheContext nearCacheContext) {
        ensureInitialized(nearCacheConfig, nearCacheContext);

        return new HiDensityNativeMemoryNearCacheRecordMap(
                        DEFAULT_INITIAL_CAPACITY, recordProcessor, storageInfo);
    }

    //CHECKSTYLE:OFF
    @Override
    protected MaxSizeChecker createNearCacheMaxSizeChecker(EvictionConfig evictionConfig,
                                                           NearCacheConfig nearCacheConfig,
                                                           NearCacheContext nearCacheContext) {
        ensureInitialized(nearCacheConfig, nearCacheContext);

        EvictionConfig.MaxSizePolicy maxSizePolicy = evictionConfig.getMaximumSizePolicy();
        if (maxSizePolicy== null) {
            throw new IllegalArgumentException("Max-Size policy cannot be null");
        }

        final long maxNativeMemory =
                ((EnterpriseSerializationService) nearCacheContext.getSerializationService())
                        .getMemoryManager().getMemoryStats().getMaxNativeMemory();
        switch (maxSizePolicy) {
            case USED_NATIVE_MEMORY_SIZE:
                return new HiDensityUsedNativeMemorySizeMaxSizeChecker(storageInfo,
                                                                       evictionConfig.getSize());
            case USED_NATIVE_MEMORY_PERCENTAGE:
                return new HiDensityUsedNativeMemoryPercentageMaxSizeChecker(storageInfo,
                                                                             evictionConfig.getSize(),
                                                                             maxNativeMemory);
            case FREE_NATIVE_MEMORY_SIZE:
                return new HiDensityFreeNativeMemorySizeMaxSizeChecker(memoryManager,
                                                                       evictionConfig.getSize());
            case FREE_NATIVE_MEMORY_PERCENTAGE:
                return new HiDensityFreeNativeMemoryPercentageMaxSizeChecker(memoryManager,
                                                                             evictionConfig.getSize(),
                                                                             maxNativeMemory);
            default:
                throw new IllegalArgumentException("Invalid max-size policy "
                        + "(" + maxSizePolicy + ") for " + getClass().getName() + " ! Only "
                        + EvictionConfig.MaxSizePolicy.ENTRY_COUNT + ", "
                        + EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE + ", "
                        + EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE + ", "
                        + EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE + ", "
                        + EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE
                        + " are supported.");
        }
    }
    //CHECKSTYLE:ON

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

    private boolean isMemoryBlockValid(MemoryBlock memoryBlock) {
        return memoryBlock != null && memoryBlock.address() != MemoryManager.NULL_ADDRESS;
    }

    private HiDensityNativeMemoryNearCacheRecord createRecord(Object value, long creationTime,
                                                              long expiryTime) {
        return createRecordInternal(value, creationTime, expiryTime, false, true);
    }

    private HiDensityNativeMemoryNearCacheRecord createRecordInternal(Object value, long creationTime,
                                                                      long expiryTime, boolean forceEvict,
                                                                      boolean retryOnOutOfMemoryError) {
        if (forceEvict) {
            forceEvict();
        }

        NativeMemoryData data = null;
        HiDensityNativeMemoryNearCacheRecord record;
        long recordAddress = MemoryManager.NULL_ADDRESS;

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
                record.setValueAddress(MemoryManager.NULL_ADDRESS);
            }

            return record;
        } catch (NativeOutOfMemoryError e) {
            // If any memory region is allocated for record, dispose it
            if (recordAddress != MemoryManager.NULL_ADDRESS) {
                recordProcessor.dispose(recordAddress);
            }
            // If any data is allocated for record, dispose it
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
        if (key instanceof Data) {
            // Because key will saved as native memory data with native memory data header
            // and this is not covered at "totalSize()" method.
            return NativeMemoryData.NATIVE_MEMORY_DATA_OVERHEAD + ((Data) key).totalSize();
        } else {
            // Memory cost for non-data typed instance is not supported.
            return 0L;
        }
    }

    @Override
    protected long getRecordStorageMemoryCost(HiDensityNativeMemoryNearCacheRecord record) {
        return record.size();
    }

    @Override
    protected HiDensityNativeMemoryNearCacheRecord valueToRecord(V value) {
        long creationTime = Clock.currentTimeMillis();
        if (timeToLiveMillis > 0) {
            return createRecord(value, creationTime, creationTime + timeToLiveMillis);
        } else {
            return createRecord(value, creationTime, NearCacheRecord.TIME_NOT_SET);
        }
    }

    @Override
    protected V recordToValue(HiDensityNativeMemoryNearCacheRecord record) {
        if (!isMemoryBlockValid(record)) {
            return null;
        }
        return (V) recordProcessor.readValue(record, true);
    }

    @Override
    protected HiDensityNativeMemoryNearCacheRecord getRecord(K key) {
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
    protected void onGet(K key, V value, HiDensityNativeMemoryNearCacheRecord record) {
        // If the record is available, put this to queue for reusing later
        if (record != null) {
            recordProcessor.enqueueRecord(record);
        }
    }

    @Override
    protected void onGetError(K key, V value, HiDensityNativeMemoryNearCacheRecord record, Throwable error) {
        // If the record is available, put this to queue for reusing later
        if (record != null) {
            recordProcessor.enqueueRecord(record);
        }
    }

    @Override
    protected void onPut(K key, V value, HiDensityNativeMemoryNearCacheRecord record,
                         HiDensityNativeMemoryNearCacheRecord oldRecord) {
        // If old record is available, dispose it since it is replaced
        if (isMemoryBlockValid(oldRecord)) {
            recordProcessor.dispose(oldRecord);
        }
        // If the record is available, put this to queue for reusing later
        if (record != null) {
            recordProcessor.enqueueRecord(record);
        }
    }

    @Override
    protected void onPutError(K key, V value, HiDensityNativeMemoryNearCacheRecord record,
                              HiDensityNativeMemoryNearCacheRecord oldRecord, Throwable error) {
        // If old record is somehow allocated, dispose it since it is not in use
        if (isMemoryBlockValid(record)) {
            recordProcessor.dispose(record);
        }
    }

    @Override
    protected void onRemove(K key, HiDensityNativeMemoryNearCacheRecord record, boolean removed) {
        // If the record is available, dispose its data and put this to queue for reusing later
        if (record != null) {
            recordProcessor.dispose(record);
            recordProcessor.enqueueRecord(record);
        }
    }

    @Override
    protected void onRemoveError(K key, HiDensityNativeMemoryNearCacheRecord record,
                                 boolean removed, Throwable error) {
        // If record has been somehow removed and if it is still valid, dispose it and its data
        if (removed && isMemoryBlockValid(record)) {
            recordProcessor.dispose(record);
            return;
        }
        // If the record is available, put this to queue for reusing later
        if (record != null) {
            recordProcessor.enqueueRecord(record);
        }
    }

    @Override
    public Object selectToSave(Object... candidates) {
        Object selectedCandidate = null;
        if (candidates != null && candidates.length > 0) {
            for (Object candidate : candidates) {
                // Give priority to Data typed candidate.
                // So there will be no extra convertion from Object to Data.
                if (candidate instanceof Data) {
                    selectedCandidate = candidate;
                    break;
                }
            }
            if (selectedCandidate != null) {
                return selectedCandidate;
            } else {
                // Select a non-null candidate
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

        return records.forceEvict(HiDensityRecordStore.DEFAULT_FORCED_EVICTION_PERCENTAGE,
                                  recordEvictionListener);
    }

    @Override
    public void doExpiration() {
        checkAvailable();

        records.evictExpiredRecords(recordEvictionListener, recordExpirationChecker);
    }

    @Override
    public void onEvict(Data key, HiDensityNativeMemoryNearCacheRecord record) {
        super.onEvict(key, record);
        nearCacheStats.decrementOwnedEntryMemoryCost(getTotalStorageMemoryCost((K) key, record));
    }

    @Override
    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    /**
     * {@link EvictionListener} implementation for listening record eviction
     */
    private class RecordEvictionListener implements EvictionListener<Data, HiDensityNativeMemoryNearCacheRecord> {

        @Override
        public void onEvict(Data key, HiDensityNativeMemoryNearCacheRecord record) {
            nearCacheStats.decrementOwnedEntryCount();
            nearCacheStats.decrementOwnedEntryMemoryCost(getTotalStorageMemoryCost((K) key, record));
        }

    };

    /**
     * {@link ExpirationChecker} implementation for checking record expiration
     */
    private class RecordExpirationChecker implements ExpirationChecker<HiDensityNativeMemoryNearCacheRecord> {

        @Override
        public boolean isExpired(HiDensityNativeMemoryNearCacheRecord record) {
            return isRecordExpired(record);
        }

    };

}
