package com.hazelcast.cache.hidensity.nearcache.impl.nativememory;

import com.hazelcast.cache.hidensity.nearcache.HiDensityNearCacheRecordStore;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheRecord;
import com.hazelcast.cache.impl.nearcache.impl.store.AbstractNearCacheRecordStore;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.hidensity.HiDensityRecordProcessor;
import com.hazelcast.hidensity.HiDensityRecordStore;
import com.hazelcast.hidensity.HiDensityStorageInfo;
import com.hazelcast.hidensity.impl.DefaultHiDensityRecordProcessor;
import com.hazelcast.hidensity.impl.SampleableEvictableHiDensityRecordMap;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.NativeMemoryData;
import com.hazelcast.spi.Callback;
import com.hazelcast.util.Clock;

/**
 * @author sozal 26/10/14
 *
 * @param <K> the type of the key stored in near-cache
 * @param <V> the type of the value stored in near-cache
 */
public class HiDensityNativeMemoryNearCacheRecordStore<K, V>
        extends AbstractNearCacheRecordStore<K, V, HiDensityNativeMemoryNearCacheRecord>
        implements HiDensityNearCacheRecordStore<K, V, HiDensityNativeMemoryNearCacheRecord> {

    private static final int DEFAULT_INITIAL_CAPACITY = 1000;

    private final MemoryManager memoryManager;
    private final HiDensityNativeMemoryNearCacheRecordAccessor recordAccessor;
    private final HiDensityRecordProcessor<HiDensityNativeMemoryNearCacheRecord> recordProcessor;
    private SampleableEvictableHiDensityRecordMap<HiDensityNativeMemoryNearCacheRecord> recordMap;

    public HiDensityNativeMemoryNearCacheRecordStore(NearCacheConfig nearCacheConfig,
                                                     NearCacheContext nearCacheContext) {
        this(nearCacheConfig, nearCacheContext,
                new NearCacheStatsImpl(),
                new HiDensityStorageInfo(nearCacheConfig.getName()));
    }

    public HiDensityNativeMemoryNearCacheRecordStore(NearCacheConfig nearCacheConfig,
                                                     NearCacheContext nearCacheContext,
                                                     NearCacheStatsImpl nearCacheStats,
                                                     HiDensityStorageInfo storageInfo) {
        super(nearCacheConfig, nearCacheContext, nearCacheStats);
        EnterpriseSerializationService serializationService =
                (EnterpriseSerializationService) nearCacheContext.getSerializationService();
        MemoryManager mm = serializationService.getMemoryManager();
        if (mm instanceof PoolingMemoryManager) {
            this.memoryManager = ((PoolingMemoryManager) mm).getGlobalMemoryManager();
        } else {
            this.memoryManager = mm;
        }
        this.recordAccessor =
                new HiDensityNativeMemoryNearCacheRecordAccessor(serializationService,
                        memoryManager);
        this.recordProcessor =
                new DefaultHiDensityRecordProcessor<HiDensityNativeMemoryNearCacheRecord>(
                        serializationService, recordAccessor,
                        memoryManager, storageInfo);
        this.recordMap =
                new SampleableEvictableHiDensityRecordMap(
                        DEFAULT_INITIAL_CAPACITY, recordProcessor,
                        createEvictionCallback(), storageInfo);
    }

    private Callback<Data> createEvictionCallback() {
        return new Callback<Data>() {
            public void notify(Data object) {
                // TODO Take action if needed ?
            }
        };
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
    protected boolean isAvailable() {
        return recordMap != null;
    }

    @Override
    protected long getKeyStorageMemoryCost(K key) {
        if (key instanceof Data) {
            return ((Data) key).totalSize();
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
        return recordMap.get(toData(key));
    }

    @Override
    protected HiDensityNativeMemoryNearCacheRecord putRecord(K key, HiDensityNativeMemoryNearCacheRecord record) {
        return recordMap.put(toData(key), record);
    }

    @Override
    protected void putToRecord(HiDensityNativeMemoryNearCacheRecord record, V value) {
        record.setValue(toNativeMemoryData(value));
    }

    @Override
    protected HiDensityNativeMemoryNearCacheRecord removeRecord(K key) {
        return recordMap.remove(toData(key));
    }

    @Override
    protected void clearRecords() {
        recordMap.clear();
    }

    @Override
    protected void destroyStore() {
        recordMap.destroy();
        // Clear reference so GC can collect it
        recordMap = null;
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
    protected void onPut(K key, V value, HiDensityNativeMemoryNearCacheRecord record, boolean newPut) {
        // If the record is available, put this to queue for reusing later
        if (record != null) {
            recordProcessor.enqueueRecord(record);
        }
    }

    @Override
    protected void onPutError(K key, V value, HiDensityNativeMemoryNearCacheRecord record,
                              boolean newPut, Throwable error) {
        // If this record has been somehow saved, dispose it
        if (newPut && isMemoryBlockValid(record)) {
            if (!recordMap.delete(toData(key))) {
                recordProcessor.dispose(record);
                return;
            }
        }
        // If the record is available, put this to queue for reusing later
        if (record != null) {
            recordProcessor.enqueueRecord(record);
        }
    }

    @Override
    protected void onRemove(K key, HiDensityNativeMemoryNearCacheRecord record, boolean removed) {
        // If the record is available, put this to queue for reusing later
        if (record != null) {
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
    public int size() {
        checkAvailable();

        return recordMap.size();
    }

    @Override
    public int forceEvict() {
        checkAvailable();

        return recordMap.forceEvict(HiDensityRecordStore.DEFAULT_FORCED_EVICT_PERCENTAGE);
    }

}
