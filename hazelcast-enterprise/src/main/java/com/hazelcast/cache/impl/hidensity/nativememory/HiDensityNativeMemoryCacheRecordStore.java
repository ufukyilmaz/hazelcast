package com.hazelcast.cache.impl.hidensity.nativememory;

import com.hazelcast.cache.impl.AbstractCacheRecordStore;
import com.hazelcast.cache.impl.CacheEntryProcessorEntry;
import com.hazelcast.cache.impl.EnterpriseCacheService;
import com.hazelcast.cache.impl.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.impl.hidensity.maxsize.HiDensityFreeNativeMemoryPercentageEvictionChecker;
import com.hazelcast.cache.impl.hidensity.maxsize.HiDensityFreeNativeMemorySizeEvictionChecker;
import com.hazelcast.cache.impl.hidensity.maxsize.HiDensityUsedNativeMemoryPercentageEvictionChecker;
import com.hazelcast.cache.impl.hidensity.maxsize.HiDensityUsedNativeMemorySizeEvictionChecker;
import com.hazelcast.cache.impl.record.CacheDataRecord;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType;
import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.comparators.NativeValueComparator;
import com.hazelcast.internal.util.comparators.ValueComparator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;
import com.hazelcast.wan.impl.CallerProvenance;

import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("checkstyle:methodcount")
public class HiDensityNativeMemoryCacheRecordStore
        extends AbstractCacheRecordStore<HiDensityNativeMemoryCacheRecord, HiDensityNativeMemoryCacheRecordMap>
        implements HiDensityCacheRecordStore<HiDensityNativeMemoryCacheRecord> {

    private static final int NATIVE_MEMORY_DEFAULT_INITIAL_CAPACITY = 128;

    protected HiDensityStorageInfo cacheInfo;
    protected HazelcastMemoryManager memoryManager;
    protected EnterpriseSerializationService serializationService;
    protected HiDensityRecordProcessor<HiDensityNativeMemoryCacheRecord> cacheRecordProcessor;

    private final ILogger logger;

    public HiDensityNativeMemoryCacheRecordStore(int partitionId, String cacheNameWithPrefix, EnterpriseCacheService cacheService,
                                                 NodeEngine nodeEngine) {
        super(cacheNameWithPrefix, partitionId, nodeEngine, cacheService);
        this.logger = nodeEngine.getLogger(getClass());
        ensureInitialized();
    }

    @Override
    protected ValueComparator getValueComparatorOf(InMemoryFormat inMemoryFormat) {
        if (inMemoryFormat == InMemoryFormat.NATIVE) {
            return NativeValueComparator.INSTANCE;
        }
        return super.getValueComparatorOf(inMemoryFormat);
    }

    private static boolean isInvalidMaxSizePolicyExceptionDisabled() {
        // by default this property does not exist, but it can be set to "true" ,e.g., for TCK tests
        // or other scenarios which don't have a max-size policy
        // example: -DdisableInvalidMaxSizePolicyException=true
        return Boolean.getBoolean(SYSTEM_PROPERTY_NAME_TO_DISABLE_INVALID_MAX_SIZE_POLICY_EXCEPTION);
    }

    /**
     * Creates an instance for checking if the maximum cache size has been reached. Supports the following policies :
     * <ul>
     * <li>{@link MaxSizePolicy#USED_NATIVE_MEMORY_SIZE}</li>
     * <li>{@link MaxSizePolicy#USED_NATIVE_MEMORY_PERCENTAGE}</li>
     * <li>{@link MaxSizePolicy#FREE_NATIVE_MEMORY_SIZE}</li>
     * <li>{@link MaxSizePolicy#FREE_NATIVE_MEMORY_PERCENTAGE}</li>
     * </ul>
     * <p>
     * If the {@code maxSizePolicy} parameter is null or the policy is not supported, returns null if the
     * {@link #SYSTEM_PROPERTY_NAME_TO_DISABLE_INVALID_MAX_SIZE_POLICY_EXCEPTION} system property is true, else throws an
     * {@link IllegalArgumentException}.
     *
     * @param size          the maximum size
     * @param maxSizePolicy the way in which the size is interpreted based on the supported policies
     * @return the instance which will check if the maximum size has been reached or null if the
     * {@code maxSizePolicy} is null or not supported and the
     * {@link #SYSTEM_PROPERTY_NAME_TO_DISABLE_INVALID_MAX_SIZE_POLICY_EXCEPTION} system property is true
     * @throws IllegalArgumentException if the {@code maxSizePolicy} is null or not supported and the
     *                                  {@link #SYSTEM_PROPERTY_NAME_TO_DISABLE_INVALID_MAX_SIZE_POLICY_EXCEPTION} system
     *                                  property is false
     */
    @Override
    protected EvictionChecker createCacheEvictionChecker(int size, MaxSizePolicy maxSizePolicy) {
        if (maxSizePolicy == null) {
            if (isInvalidMaxSizePolicyExceptionDisabled()) {
                // don't throw exception, just ignore max-size policy
                return null;
            } else {
                throw new IllegalArgumentException("Max-size policy cannot be null");
            }
        }

        long maxNativeMemory = ((EnterpriseSerializationService) nodeEngine.getSerializationService())
                .getMemoryManager().getMemoryStats().getMaxNative();
        switch (maxSizePolicy) {
            case USED_NATIVE_MEMORY_SIZE:
                return new HiDensityUsedNativeMemorySizeEvictionChecker(cacheInfo, size);
            case USED_NATIVE_MEMORY_PERCENTAGE:
                return new HiDensityUsedNativeMemoryPercentageEvictionChecker(cacheInfo, size, maxNativeMemory);
            case FREE_NATIVE_MEMORY_SIZE:
                return new HiDensityFreeNativeMemorySizeEvictionChecker(memoryManager, size);
            case FREE_NATIVE_MEMORY_PERCENTAGE:
                return new HiDensityFreeNativeMemoryPercentageEvictionChecker(memoryManager, size, maxNativeMemory);
            default:
                if (isInvalidMaxSizePolicyExceptionDisabled()) {
                    // don't throw exception, just ignore max-size policy
                    return null;
                } else {
                    throw new IllegalArgumentException("Invalid max-size policy "
                            + '(' + maxSizePolicy + ") for " + getClass().getName() + "! Only "
                            + MaxSizePolicy.USED_NATIVE_MEMORY_SIZE + ", "
                            + MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE + ", "
                            + MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE + ", "
                            + MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE
                            + " are supported.");
                }
        }
    }

    @Override
    protected void forceRemoveRecord(Data key) {
        removeRecord(key);
        // the key can be disposed now as it is not used later
        // also, disposal by the cacheRecordProcessor ensures memory stats are updated
        // to correctly reflect free memory (while deferred disposal does not update storage info)
        cacheRecordProcessor.disposeData(key);
    }

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
                // it is created from its partition specific thread
                // since memory manager is `PoolingMemoryManager`, this means that
                // this record store will always use its partition specific `ThreadLocalPoolingMemoryManager`
                // in this case, no need to find its partition specific `ThreadLocalPoolingMemoryManager` inside
                // `GlobalPoolingMemoryManager` for every memory allocation/free every time
                // so, we explicitly use its partition specific `ThreadLocalPoolingMemoryManager` directly
                memoryManager = ((PoolingMemoryManager) memoryManager).getMemoryManager();
            }
        }
        if (memoryManager == null) {
            throw new IllegalStateException("Native memory must be enabled to use Hi-Density storage!");
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
    @SuppressWarnings("checkstyle:npathcomplexity")
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
                cacheRecordMap = createMapInternal(capacity);
                break;
            } catch (NativeOutOfMemoryError e) {
                oome = e;
            }
            capacity = capacity >> 1;
        } while (capacity > 0);

        if (cacheRecordMap == null) {
            throw oome;
        }
        return cacheRecordMap;
    }

    protected HiDensityNativeMemoryCacheRecordMap createMapInternal(int capacity) {
        return new HiDensityNativeMemoryCacheRecordMap(capacity, cacheRecordProcessor, cacheInfo);
    }

    @Override
    protected CacheEntryProcessorEntry createCacheEntryProcessorEntry(Data key,
                                                                      HiDensityNativeMemoryCacheRecord record,
                                                                      long now, int completionId) {
        return new HiDensityNativeMemoryCacheEntryProcessorEntry(key, record, this, now, completionId);
    }

    boolean isMemoryBlockValid(MemoryBlock memoryBlock) {
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

    @Override
    public CacheRecord merge(CacheMergeTypes mergingEntry,
                             SplitBrainMergePolicy<Data, CacheMergeTypes> mergePolicy, CallerProvenance callerProvenance) {
        return toHeapCacheRecord((HiDensityNativeMemoryCacheRecord) super.merge(mergingEntry, mergePolicy, callerProvenance));
    }

    @Override
    public final CacheRecord toHeapCacheRecord(HiDensityNativeMemoryCacheRecord record) {
        if (!isMemoryBlockValid(record)) {
            return null;
        }
        CacheRecord cacheRecord = new CacheDataRecord();
        cacheRecord.setCreationTime(record.getCreationTime());
        cacheRecord.setLastAccessTime(record.getLastAccessTime());
        cacheRecord.setHits(record.getHits());
        cacheRecord.setExpirationTime(record.getExpirationTime());
        cacheRecord.setValue(toHeapData(record.getValue()));
        cacheRecord.setExpiryPolicy(toHeapData(record.getExpiryPolicy()));
        return cacheRecord;
    }

    @Override
    public void disposeDeferredBlocks() {
        getRecordProcessor().disposeDeferredBlocks();
    }

    private HiDensityNativeMemoryCacheRecord toNativeMemoryCacheRecord(CacheRecord record) {
        if (record instanceof HiDensityNativeMemoryCacheRecord) {
            return (HiDensityNativeMemoryCacheRecord) record;
        }
        HiDensityNativeMemoryCacheRecord nativeMemoryRecord =
                createRecord(record.getValue(), record.getCreationTime(), record.getExpirationTime());
        nativeMemoryRecord.setLastAccessTime(record.getLastAccessTime());
        nativeMemoryRecord.setHits(record.getHits());
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
    public HiDensityRecordProcessor<HiDensityNativeMemoryCacheRecord> getRecordProcessor() {
        return cacheRecordProcessor;
    }

    @Override
    protected HiDensityNativeMemoryCacheRecord createRecord(Object value, long creationTime, long expiryTime) {
        evictIfRequired();
        markExpirable(expiryTime);
        return createRecordInternal(value, creationTime, expiryTime, newSequence());
    }

    @Override
    protected void initExpirationIterator() {
        if (expirationIterator == null || !expirationIterator.hasNext()) {
            expirationIterator = records.entryIter(false);
        }
    }

    final HiDensityNativeMemoryCacheRecord createRecordInternal(Object value, long creationTime,
                                                                long expiryTime, long sequence) {

        NativeMemoryData data = null;
        HiDensityNativeMemoryCacheRecord record;
        long recordAddress = NULL_PTR;

        try {
            recordAddress = cacheRecordProcessor.allocate(HiDensityNativeMemoryCacheRecord.SIZE);
            record = cacheRecordProcessor.newRecord();
            record.reset(recordAddress);
            record.setLastAccessTime(HiDensityNativeMemoryCacheRecord.TIME_NOT_AVAILABLE);
            record.setSequence(sequence);

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
            // If any memory region is allocated for record, dispose it.
            if (recordAddress != NULL_PTR) {
                cacheRecordProcessor.dispose(recordAddress);
            }
            // If any data is allocated for record, dispose it.
            if (isMemoryBlockValid(data)) {
                cacheRecordProcessor.disposeData(data);
            }

            throw e;
        }
    }

    long newSequence() {
        return 0L;
    }

    @SuppressWarnings("checkstyle:parameternumber")
    @Override
    protected void onCreateRecordError(Data key, Object value, long expiryTime, long now,
                                       boolean disableWriteThrough, int completionId, UUID origin,
                                       HiDensityNativeMemoryCacheRecord record, Throwable error) {
        if (isMemoryBlockValid(record)) {
            /*
             * Record might be put somehow before error, so in case of error we should revert it.
             * Note that, we don't use `delete` but `remove`, because we want to dispose it explicitly.
             *
             * If `key` is converted to `NativeMemoryData` inside record map, it is disposed there, not here.
             * But if it is already `NativeMemoryData`, it is disposed inside operation itself.
             * So no need to explicitly disposing at here in case of error.
             */
            final HiDensityNativeMemoryCacheRecord removed = records.remove(key);
            if (removed != null) {
                cacheService.getEventJournal().writeRemoveEvent(eventJournalConfig, objectNamespace, partitionId,
                        key, recordToValue(record));
            }
            if (value instanceof NativeMemoryData) {
                // if value is allocated outside of record store, disposing value is its responsibility
                // so just dispose record which is allocated here
                cacheRecordProcessor.free(record.address(), record.size());
                record.reset(NULL_PTR);
            } else {
                cacheRecordProcessor.dispose(record);
            }
        }
    }

    @Override
    protected boolean evictIfExpired(Data key, HiDensityNativeMemoryCacheRecord record, long now) {
        if (super.evictIfExpired(key, record, now)) {
            getRecordProcessor().addDeferredDispose((NativeMemoryData) key);
            return true;
        }
        return false;
    }

    @Override
    public void evictExpiredEntries(int expirationPercentage) {
        super.evictExpiredEntries(expirationPercentage);
        getRecordProcessor().disposeDeferredBlocks();
    }

    @Override
    protected void onProcessExpiredEntry(Data key, HiDensityNativeMemoryCacheRecord record, long expiryTime,
                                         long now, UUID source, UUID origin) {
        super.onProcessExpiredEntry(key, record, expiryTime, now, source, origin);
        if (isMemoryBlockValid(record)) {
            cacheRecordProcessor.dispose(record);
        }
    }

    @Override
    protected void onUpdateRecord(Data key, HiDensityNativeMemoryCacheRecord record,
                                  Object value, Data oldDataValue) {
        super.onUpdateRecord(key, record, value, oldDataValue);
        // if there is valid old value, dispose it
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
        // if there is valid new value and it is created at here, dispose it
        if (newDataValue != null && newDataValue instanceof NativeMemoryData && newDataValue != value) {
            NativeMemoryData nativeMemoryData = (NativeMemoryData) newDataValue;
            if (isMemoryBlockValid(nativeMemoryData)) {
                cacheRecordProcessor.disposeData(nativeMemoryData);
            }
        }
    }

    @Override
    protected void onDeleteRecord(Data key, HiDensityNativeMemoryCacheRecord record, boolean deleted) {
        // if record is deleted and if this record is valid, dispose it and its data
        if (deleted && isMemoryBlockValid(record)) {
            cacheRecordProcessor.dispose(record);
        }
    }

    @Override
    protected void onUpdateExpiryPolicy(Data key, HiDensityNativeMemoryCacheRecord record, Data oldDataExpiryPolicy) {
        super.onUpdateExpiryPolicy(key, record, oldDataExpiryPolicy);
        if (oldDataExpiryPolicy != null) {
            cacheRecordProcessor.disposeData(oldDataExpiryPolicy);
        }
    }

    @Override
    protected void onUpdateExpiryPolicyError(Data key, HiDensityNativeMemoryCacheRecord record, Data oldDataExpiryPolicy) {
        super.onUpdateExpiryPolicyError(key, record, oldDataExpiryPolicy);
        if (oldDataExpiryPolicy instanceof NativeMemoryData) {
            NativeMemoryData nativeMemoryData = (NativeMemoryData) oldDataExpiryPolicy;
            if (isMemoryBlockValid(nativeMemoryData)) {
                record.setExpiryPolicy(nativeMemoryData);
            }
        }
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private HiDensityNativeMemoryCacheRecord putRecordInternal(Data key, CacheRecord record, boolean updateJournal) {
        if (!records.containsKey(key)) {
            evictIfRequired();
        }

        HiDensityNativeMemoryCacheRecord newRecord = toNativeMemoryCacheRecord(record);
        HiDensityNativeMemoryCacheRecord oldRecord;
        try {
            oldRecord = doPutRecord(key, newRecord, SOURCE_NOT_AVAILABLE, updateJournal);
        } catch (Throwable t) {
            if (newRecord != record) {
                // if record is created at here, dispose it before throwing error
                cacheRecordProcessor.dispose(newRecord);
            }
            throw ExceptionUtil.rethrow(t);
        }

        // no exception means that put is successful

        // add key to deferred list if it is `NativeMemoryData`, then operation will dispose it later
        // since old record is null, this means that put was handled as a replace
        // so key is not stored actually
        if (oldRecord != null && key instanceof NativeMemoryData) {
            NativeMemoryData nativeKey = (NativeMemoryData) key;
            if (isMemoryBlockValid(nativeKey)) {
                cacheRecordProcessor.addDeferredDispose(nativeKey);
            }
        }

        // if old record does not exist (null) and there is no convertion for key,
        // add key memory usage to used memory explicitly
        if (oldRecord == null && key instanceof NativeMemoryData) {
            long size = cacheRecordProcessor.getSize((NativeMemoryData) key);
            cacheRecordProcessor.increaseUsedMemory(size);
        }

        // if the new record is already native memory based, this means that there is no convertion,
        // add record and value memory usage to used memory explicitly
        if (record instanceof HiDensityNativeMemoryCacheRecord) {
            long size = cacheRecordProcessor.getSize(newRecord);
            size += cacheRecordProcessor.getSize(newRecord.getValue());
            cacheRecordProcessor.increaseUsedMemory(size);
        }

        return oldRecord;
    }

    @Override
    public void putRecord(Data key, CacheRecord record, boolean updateJournal) {
        HiDensityNativeMemoryCacheRecord oldRecord = putRecordInternal(key, record, updateJournal);
        if (isMemoryBlockValid(oldRecord)) {
            cacheRecordProcessor.dispose(oldRecord);
        }
    }

    @Override
    public CacheRecord removeRecord(Data key) {
        HiDensityNativeMemoryCacheRecord removedRecord = records.remove(key);
        CacheRecord recordToReturn = null;
        // if removed record is valid, first get a heap based copy of it and dispose it
        if (isMemoryBlockValid(removedRecord)) {
            recordToReturn = toHeapCacheRecord(removedRecord);
            cacheRecordProcessor.dispose(removedRecord);
        }
        if (removedRecord != null) {
            cacheService.getEventJournal().writeRemoveEvent(eventJournalConfig, objectNamespace, partitionId,
                    key, recordToReturn != null ? recordToReturn.getValue() : null);
        }
        return recordToReturn;
    }

    private void onAccess(long now, HiDensityNativeMemoryCacheRecord record) {
        if (isEvictionEnabled()) {
            record.setLastAccessTime(now);
            record.incrementHits();
        }
    }

    @Override
    @SuppressWarnings({
            "checkstyle:parameternumber",
            "checkstyle:cyclomaticcomplexity",
            "checkstyle:npathcomplexity"
    })
    protected void onPut(Data key, Object value, ExpiryPolicy expiryPolicy, UUID caller,
                         boolean getValue, boolean disableWriteThrough, HiDensityNativeMemoryCacheRecord record,
                         Object oldValue, boolean isExpired, boolean isNewPut, boolean isSaveSucceed) {
        // old value is disposed at `onUpdateRecord`

        // if put is successful
        if (isSaveSucceed) {
            // if key is `NativeMemoryData`, since there is no convertion,
            // add its memory to used memory explicitly
            if (isNewPut && key instanceof NativeMemoryData) {
                long size = cacheRecordProcessor.getSize((NativeMemoryData) key);
                cacheRecordProcessor.increaseUsedMemory(size);
            }
            // if value is `NativeMemoryData`, since there is no convertion,
            // add its memory to used memory explicitly
            if (value instanceof NativeMemoryData) {
                long size = cacheRecordProcessor.getSize((NativeMemoryData) value);
                cacheRecordProcessor.increaseUsedMemory(size);
            }
        }

        if (isSaveSucceed) {
            if (!isNewPut && key instanceof NativeMemoryData) {
                // if save is successful as new put, this means that key is not stored
                // so add key to deferred list if it is `NativeMemoryData`, then operation will dispose it later
                NativeMemoryData nativeKey = (NativeMemoryData) key;
                if (isMemoryBlockValid(nativeKey)) {
                    cacheRecordProcessor.addDeferredDispose(nativeKey);
                }
            }
        } else {
            // if save is not successful, this means that key is not stored
            // so add key to deferred list if it is `NativeMemoryData`, then operation will dispose it later
            if (key instanceof NativeMemoryData) {
                NativeMemoryData nativeKey = (NativeMemoryData) key;
                if (isMemoryBlockValid(nativeKey)) {
                    cacheRecordProcessor.addDeferredDispose(nativeKey);
                }
            }
            // if save is not successful, this means that value is not stored
            // so add key to deferred list if it is `NativeMemoryData`, then operation will dispose it later
            if (value instanceof NativeMemoryData) {
                NativeMemoryData nativeValue = (NativeMemoryData) value;
                if (isMemoryBlockValid(nativeValue)) {
                    cacheRecordProcessor.addDeferredDispose(nativeValue);
                }
            }
        }
    }

    protected void onOwn(Data key, Object value, long ttlMillis, HiDensityNativeMemoryCacheRecord record,
                         NativeMemoryData oldValueData, boolean isNewPut, boolean disableDeferredDispose) {
        // dispose old value if exist
        if (oldValueData != null) {
            cacheRecordProcessor.disposeData(oldValueData);
        }

        // if there is no new put, this means that key is not used
        if (!isNewPut && key instanceof NativeMemoryData) {
            if (disableDeferredDispose) {
                // if deferred dispose is disabled, disabling unused key is our responsibility
                // so dispose unused key at here
                // but since it is allocated outside (it is already `NativeMemoryData`)
                // as not through `cacheRecordProcessor` but serialization service,
                // don't dispose it through `cacheRecordProcessor`
                serializationService.disposeData(key);
            } else {
                // add key to deferred list if it is `NativeMemoryData`, then operation will dispose it later
                cacheRecordProcessor.addDeferredDispose((NativeMemoryData) key);
            }
        }

        // if key is `NativeMemoryData`, since there is no convertion,
        // add its memory to used memory explicitly
        if (isNewPut && key instanceof NativeMemoryData) {
            long size = cacheRecordProcessor.getSize((NativeMemoryData) key);
            cacheRecordProcessor.increaseUsedMemory(size);
        }

        // if value is `NativeMemoryData`, since there is no convertion,
        // add its memory to used memory explicitly
        if (value instanceof NativeMemoryData) {
            long size = cacheRecordProcessor.getSize((NativeMemoryData) value);
            cacheRecordProcessor.increaseUsedMemory(size);
        }
    }

    protected void onOwnError(Data key, Object value, long ttlMillis, HiDensityNativeMemoryCacheRecord record,
                              NativeMemoryData oldValueData, boolean isNewPut,
                              boolean disableDeferredDispose, Throwable error) {
        // if record is created
        if (isNewPut && isMemoryBlockValid(record)) {
            /*
             * Record might be put somehow before error, so in case of error we should revert it.
             * Note that, we don't use `delete` but `remove`, because we want to dispose it explicitly.
             * If `key` is converted to `NativeMemoryData` inside record map, it is disposed there, not here.
             * But if it is already `NativeMemoryData`, it is disposed inside operation itself.
             * So no need to explicitly disposing at here in case of error.
             */
            final HiDensityNativeMemoryCacheRecord removed = records.remove(key);
            if (removed != null) {
                cacheService.getEventJournal().writeRemoveEvent(eventJournalConfig, objectNamespace, partitionId,
                        key, recordToValue(removed));
            }
            if (value instanceof NativeMemoryData) {
                record.setValue(null);
                // if value is allocated outside of record store, disposing value is its responsibility
                // so just dispose record which is allocated here
                cacheRecordProcessor.free(record.address(), record.size());
                record.reset(NULL_PTR);
            } else {
                cacheRecordProcessor.dispose(record);
            }
        }
    }

    @Override
    public CacheRecord putBackup(Data key, Object value, long creationTime, ExpiryPolicy expiryPolicy) {
        expiryPolicy = getExpiryPolicy(null, expiryPolicy);
        long ttl = expiryPolicyToTTL(expiryPolicy);
        evictIfRequired();
        return own(key, value, ttl, creationTime, false, true);
    }

    private long expiryPolicyToTTL(ExpiryPolicy expiryPolicy) {
        if (expiryPolicy == null) {
            return CacheRecord.TIME_NOT_AVAILABLE;
        }
        try {
            Duration expiryDuration = expiryPolicy.getExpiryForCreation();
            if (expiryDuration == null || expiryDuration.isEternal()) {
                return CacheRecord.TIME_NOT_AVAILABLE;
            }
            long durationAmount = expiryDuration.getDurationAmount();
            TimeUnit durationTimeUnit = expiryDuration.getTimeUnit();
            return TimeUnit.MILLISECONDS.convert(durationAmount, durationTimeUnit);
        } catch (Exception e) {
            return CacheRecord.TIME_NOT_AVAILABLE;
        }
    }

    @Override
    public CacheRecord putReplica(Data key, Object value, long creationTime, long ttlMillis) {
        return own(key, value, ttlMillis, creationTime, true, false);
    }

    private CacheRecord own(Data key, Object value, long ttlMillis, long creationTime,
                            boolean disableDeferredDispose, boolean updateJournal) {
        long now = Clock.currentTimeMillis();
        HiDensityNativeMemoryCacheRecord record = null;
        NativeMemoryData oldValueData = null;
        boolean isNewPut = false;

        try {
            record = records.get(key);
            if (record == null) {
                isNewPut = true;
                record = createRecord(value, creationTime, Long.MAX_VALUE);
                records.put(key, record);
                if (updateJournal) {
                    cacheService.getEventJournal().writeCreatedEvent(eventJournalConfig, objectNamespace, partitionId,
                            key, recordToValue(record));
                }
            } else {
                oldValueData = record.getValue();
                updateRecordValue(record, toNativeMemoryData(value));
                record.setCreationTime(creationTime);
                if (updateJournal) {
                    cacheService.getEventJournal().writeUpdateEvent(eventJournalConfig, objectNamespace, partitionId,
                            key, toHeapData(oldValueData), recordToValue(record));
                }
            }

            onAccess(now, record);
            record.setTtlMillis(ttlMillis);

            onOwn(key, value, ttlMillis, record, oldValueData, isNewPut, disableDeferredDispose);

            return record;
        } catch (Throwable error) {
            onOwnError(key, value, ttlMillis, record, oldValueData, isNewPut, disableDeferredDispose, error);

            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    protected void onPutIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy, UUID caller,
                                 boolean disableWriteThrough, HiDensityNativeMemoryCacheRecord record,
                                 boolean isExpired, boolean isSaveSucceed) {
        // if put is successful
        if (isSaveSucceed) {
            // if key is `NativeMemoryData`, since there is no convertion,
            // add its memory to used memory explicitly
            if (key instanceof NativeMemoryData) {
                long size = cacheRecordProcessor.getSize((NativeMemoryData) key);
                cacheRecordProcessor.increaseUsedMemory(size);
            }
            // if value is `NativeMemoryData`, since there is no convertion,
            // add its memory to used memory explicitly
            if (value instanceof NativeMemoryData) {
                long size = cacheRecordProcessor.getSize((NativeMemoryData) value);
                cacheRecordProcessor.increaseUsedMemory(size);
            }
        }
    }

    @Override
    public boolean putIfAbsent(Data key, Object value, UUID caller, int completionId) {
        return putIfAbsent(key, value, defaultExpiryPolicy, caller, completionId);
    }

    @SuppressWarnings("checkstyle:parameternumber")
    @Override
    protected void onReplace(Data key, Object oldValue, Object newValue, ExpiryPolicy expiryPolicy,
                             UUID caller, boolean getValue, HiDensityNativeMemoryCacheRecord record,
                             boolean isExpired, boolean replaced) {
        // old value is disposed at `onUpdateRecord`

        // if replace is successful
        if (replaced) {
            // if value is `NativeMemoryData`, since there is no convertion,
            // add its memory to used memory explicitly
            if (newValue instanceof NativeMemoryData) {
                long size = cacheRecordProcessor.getSize((NativeMemoryData) newValue);
                cacheRecordProcessor.increaseUsedMemory(size);
            }
        }
    }

    @Override
    public boolean replace(Data key, Object value, UUID caller, int completionId) {
        return replace(key, value, defaultExpiryPolicy, caller, completionId);
    }

    @Override
    public boolean replace(Data key, Object oldValue, Object newValue, UUID caller, int completionId) {
        return replace(key, oldValue, newValue, defaultExpiryPolicy, caller, completionId);
    }

    @Override
    protected void onRemoveError(Data key, Object value, UUID caller, boolean getValue,
                                 HiDensityNativeMemoryCacheRecord record, boolean removed, Throwable error) {
        // if record has been somehow removed and if it is still valid, dispose it and its data
        if (removed && isMemoryBlockValid(record)) {
            cacheRecordProcessor.dispose(record);
            return;
        }
    }

    @Override
    public int forceEvict() {
        int evictedCount = records.forceEvict(HiDensityCacheRecordStore.DEFAULT_FORCED_EVICTION_PERCENTAGE, this);
        if (isStatisticsEnabled() && evictedCount > 0) {
            statistics.increaseCacheEvictions(evictedCount);
        }
        if (cacheInfo.increaseForceEvictionCount() == 1) {
            logger.warning("Forced eviction invoked for the first time for Cache[name=" + getName() + "]");
        }
        cacheInfo.increaseForceEvictedEntryCount(evictedCount);
        return evictedCount;
    }

    @Override
    public void clear() {
        HazelcastMemoryManager memoryManager = serializationService.getMemoryManager();
        if (memoryManager == null || memoryManager.isDisposed()) {
            // otherwise will cause a SIGSEGV
            return;
        }
        super.clear();
    }

    @Override
    public void close(boolean onShutdown) {
        if (shouldExplicitlyClear(onShutdown)) {
            clear();
        } else {
            destroyEventJournal();
        }
        records.dispose();
        closeListeners();
    }

    boolean shouldExplicitlyClear(boolean onShutdown) {
        NativeMemoryConfig nativeMemoryConfig = nodeEngine.getConfig().getNativeMemoryConfig();
        return !onShutdown
                || (nativeMemoryConfig != null && nativeMemoryConfig.getAllocatorType() != MemoryAllocatorType.POOLED);
    }

    @Override
    protected void onDestroy() {
        records.dispose();
    }

    @Override
    public void destroy() {
        HazelcastMemoryManager memoryManager = serializationService.getMemoryManager();
        if (memoryManager == null || memoryManager.isDisposed()) {
            // otherwise will cause a SIGSEGV
            return;
        }
        super.destroy();
    }
}
