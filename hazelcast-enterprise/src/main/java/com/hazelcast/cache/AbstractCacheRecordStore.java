package com.hazelcast.cache;

import com.hazelcast.cache.enterprise.CacheHashMap;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.memory.MemoryBlockAccessor;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.error.OffHeapOutOfMemoryError;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.OffHeapData;
import com.hazelcast.nio.serialization.OffHeapDataUtil;
import com.hazelcast.spi.Callback;
import com.hazelcast.util.Clock;

import java.util.ArrayDeque;
import java.util.Queue;

// TODO: needs refactor and cleanup..
public abstract class AbstractCacheRecordStore implements ICacheRecordStore {

    static final long NULL_PTR = MemoryManager.NULL_ADDRESS;
    static final int MIN_FORCED_EVICT_PERCENTAGE = 10;

    protected final CacheHashMap map;
    protected final EnterpriseSerializationService ss;
    protected final MemoryManager memoryManager;
    protected final CacheRecordAccessor cacheRecordService;

    private final long defaultTTL;
    private final EvictionPolicy evictionPolicy;
    private final boolean evictionEnabled;
    private final int evictionPercentage;
    private final float evictionThreshold;

    private volatile boolean hasTtl;

    protected AbstractCacheRecordStore(EnterpriseSerializationService ss, int initialCapacity, int ttl, EvictionPolicy evictionPolicy,
            int evictionPercentage, int evictionThresholdPercentage) {
        this.ss = ss;
        this.evictionPolicy = evictionPolicy;
        cacheRecordService = new CacheRecordAccessor(ss);
        this.map = new CacheHashMap(initialCapacity, ss, cacheRecordService, createEvictionCallback());
        this.memoryManager = ss.getMemoryManager();

        this.defaultTTL = ttl * 1000L;
        this.evictionEnabled = evictionPolicy != EvictionPolicy.NONE;
        this.evictionPercentage = evictionPercentage;
        this.evictionThreshold = (float) Math.max(1, 100 - evictionThresholdPercentage) / 100;

        hasTtl = defaultTTL > 0;
    }

    public AbstractCacheRecordStore(EnterpriseSerializationService serializationService, CacheConfig cacheConfig, int initialCapacity) {
        this(serializationService, initialCapacity, 0, cacheConfig.getEvictionPolicy(),
//                cacheConfig.getEvictionPercentage(), cacheConfig.getEvictionThresholdPercentage());
                10, 95);
    }

    protected abstract Callback<Data> createEvictionCallback();

    protected Data get(Data key) {
        long now = Clock.currentTimeMillis();
        OffHeapData value = null;
        CacheRecord record = map.get(key);
        if (record != null) {
            long creationTime = record.getCreationTime();
            int ttl = record.getTtlMillis();
            if (ttl <= 0 || creationTime + ttl > now) {
                onAccess(now, record, creationTime);
                long valueAddress = record.getValueAddress();
                value = cacheRecordService.readData(valueAddress);
                cacheRecordService.enqueueRecord(record);
            } else {
                map.delete(key);
            }
        }
        // TODO: avoid free() until read is completed!
        try {
            return ss.convertData(value, DataType.HEAP);
        } finally {
            if (value != null) {
                cacheRecordService.enqueueData(value);
            }
        }
    }

    private void onAccess(long now, CacheRecord record, long creationTime) {
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

    public boolean contains(Data key) {
        return map.containsKey(key);
    }

    protected void put(Data key, Data value, long ttlMillis, String caller) {
        put0(key, value, ttlMillis, false, caller);
    }

    protected Data getAndPut(Data key, Data value, long ttlMillis, String caller) {
        return put0(key, value, ttlMillis, true, caller);
    }

    private Data put0(Data key, Data value, long ttlMillis, boolean getValue, String caller) {
        long now = Clock.currentTimeMillis();
        evictIfRequired(now);
        long creationTime;
        CacheRecord record = null;
        OffHeapData newValue = null;
        Data oldValue = null;
        boolean newPut = false;

        try {
            record = map.get(key);
            if (record == null) {
                record = createNewRecord(now);
                creationTime = now;
                map.set(key, record);
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

            newValue = ss.convertData(value, DataType.OFFHEAP);
            if (getValue) {
                OffHeapData current = cacheRecordService.readData(record.getValueAddress());
                // TODO: avoid free() until read is completed!
                oldValue = ss.convertData(current, DataType.HEAP);
                cacheRecordService.disposeData(current);
            } else {
                cacheRecordService.disposeValue(record);
            }
            record.setValueAddress(newValue.address());

            onAccess(now, record, creationTime);
            if (newPut) {
                record.resetAccessHit();
            }

            ttlMillis = ttlMillis <= 0 ? defaultTTL : ttlMillis;
            ttlMillis = ttlMillis < Integer.MAX_VALUE ? ttlMillis : Integer.MAX_VALUE;
            if (!hasTtl && ttlMillis > 0) {
                hasTtl = true;
            }
            record.setTtlMillis((int) ttlMillis);

            cacheRecordService.enqueueRecord(record);
        } catch (OffHeapOutOfMemoryError e) {
            if (newPut && record != null && record.address() != NULL_PTR) {
                if (!map.delete(key)) {
                    cacheRecordService.dispose(record);
                }
            }
            if (newValue != null && newValue.address() != NULL_PTR) {
                cacheRecordService.disposeData(newValue);
            }
            throw e;
        }
        return oldValue;
    }

    protected void own(Data key, Data value, long ttlMillis) {
        put0(key, value, ttlMillis, false, null);
    }

    protected abstract void onEntryInvalidated(Data key, String source);

    private CacheRecord createNewRecord(long now) {
        long address = memoryManager.allocate(CacheRecord.SIZE);
        CacheRecord record = cacheRecordService.newRecord();
        record.reset(address);
        record.setCreationTime(now);
        record.setValueAddress(NULL_PTR);
        return record;
    }

    public int evictIfRequired(long now) {
        if (evictionEnabled) {
            MemoryStats memoryStats = memoryManager.getMemoryStats();
            if (isEvictionRequired(memoryStats)) {
                return map.evictRecords(evictionPercentage, evictionPolicy);
            }
        }
        return 0;
    }

    public int forceEvict() {
        int percentage = Math.max(MIN_FORCED_EVICT_PERCENTAGE, evictionPercentage);
        int evicted = 0;
        if (hasTTL()) {
            evicted = map.evictExpiredRecords(100);
        }
        evicted += map.evictRecords(percentage, EvictionPolicy.RANDOM);
        return evicted;
    }

    private boolean isEvictionRequired(MemoryStats memoryStats) {
        return memoryStats.getMaxOffHeap() * evictionThreshold > memoryStats.getFreeOffHeap();
    }

    public Data getAndRemove(Data key, String caller) {
        Data oldValue = null;
        CacheRecord record = map.remove(key);
        if (record != null) {
            onEntryInvalidated(key, caller);
            OffHeapData oldBinary = cacheRecordService.readData(record.getValueAddress());
            oldValue = ss.convertData(oldBinary, DataType.HEAP);
            cacheRecordService.dispose(record);
        }
        return oldValue;
    }

    public boolean remove(Data key, String caller) {
        boolean deleted = map.delete(key);
        if (deleted) {
            onEntryInvalidated(key, caller);
        }
        return deleted;
    }

    protected boolean remove(Data key, Data value, String caller) {
        boolean deleted = false;
        CacheRecord record = map.get(key);
        if (record != null) {
            if (OffHeapDataUtil.equals(record.getValueAddress(), value)) {
                onEntryInvalidated(key, caller);
                map.delete(key);
                deleted = true;
            }
            cacheRecordService.enqueueRecord(record);
        }
        return deleted;
    }

    protected boolean putIfAbsent(Data key, Data value, String caller) {
        long now = Clock.currentTimeMillis();
        evictIfRequired(now);
        CacheRecord record = null;
        OffHeapData newValue = null;

        try {
            record = map.get(key);
            if (record != null) {
                long creationTime = record.getCreationTime();
                int ttl = record.getTtlMillis();
                if (ttl <= 0 || creationTime + ttl > now) {
                    return false;
                }
                cacheRecordService.disposeValue(record);
            } else {
                record = createNewRecord(now);
                map.set(key, record);
            }

            record.setCreationTime(now);
            record.setAccessTimeDiff(0);
            record.setTtlMillis(-1);

            newValue = ss.convertData(value, DataType.OFFHEAP);
            record.setValueAddress(newValue.address());

            cacheRecordService.enqueueRecord(record);
        } catch (OffHeapOutOfMemoryError e) {
            if (record != null && record.address() != NULL_PTR) {
                if (!map.delete(key)) {
                    cacheRecordService.dispose(record);
                }
            }
            if (newValue != null && newValue.address() != NULL_PTR) {
                cacheRecordService.disposeData(newValue);
            }
            throw e;
        }
        return true;
    }

    protected boolean replace(Data key, Data value, String caller) {
        long now = Clock.currentTimeMillis();
        evictIfRequired(now);

        CacheRecord record = map.get(key);
        if (record != null) {
            onEntryInvalidated(key, caller);
            long creationTime = record.getCreationTime();
            int ttl = record.getTtlMillis();
            if (ttl <= 0 || creationTime + ttl > now) {
                OffHeapData newValue = ss.convertData(value, DataType.OFFHEAP);
                cacheRecordService.disposeValue(record);
                record.setValueAddress(newValue.address());

                onAccess(now, record, creationTime);

                cacheRecordService.enqueueRecord(record);
                return true;
            } else {
                cacheRecordService.dispose(record);
            }
        }
        return false;
    }

    protected  boolean replace(Data key, Data oldValue, Data newValue, String caller) {
        long now = Clock.currentTimeMillis();
        evictIfRequired(now);
        CacheRecord record = map.get(key);
        if (record != null) {
            long creationTime = record.getCreationTime();
            int ttl = record.getTtlMillis();
            if (ttl <= 0 || creationTime + ttl > now) {
                long currentValueAddress = record.getValueAddress();
                if (OffHeapDataUtil.equals(currentValueAddress, oldValue)) {
                    onEntryInvalidated(key, caller);
                    OffHeapData v = ss.convertData(newValue, DataType.OFFHEAP);
                    cacheRecordService.disposeValue(record);
                    record.setValueAddress(v.address());

                    onAccess(now, record, creationTime);
                    return true;
                }
                cacheRecordService.enqueueRecord(record);
            } else {
                onEntryInvalidated(key, caller);
                cacheRecordService.dispose(record);
            }
        }
        return false;
    }

    protected  Object getAndReplace(Data key, Data value, String caller) {
        long now = Clock.currentTimeMillis();
        evictIfRequired(now);

        Data oldValue = null;
        CacheRecord record = map.get(key);
        if (record != null) {
            onEntryInvalidated(key, caller);
            long creationTime = record.getCreationTime();
            int ttl = record.getTtlMillis();
            if (ttl <= 0 || creationTime + ttl > now) {

                OffHeapData newValue = ss.convertData(value, DataType.OFFHEAP);

                OffHeapData oldBinary = cacheRecordService.readData(record.getValueAddress());
                oldValue = ss.convertData(oldBinary, DataType.HEAP);
                cacheRecordService.disposeData(oldBinary);

                record.setValueAddress(newValue.address());

                onAccess(now, record, creationTime);

                cacheRecordService.enqueueRecord(record);
            } else {
                cacheRecordService.dispose(record);
            }
        }
        return oldValue;
    }

    public final boolean hasTTL() {
        return hasTtl;
    }

    public final int size() {
        return map.size();
    }

    public final void clear() {
        onClear();
        map.clear();
    }

    protected abstract void onClear();

    public final void destroy() {
        clear();
        onDestroy();
        map.destroy();
    }

    protected abstract void onDestroy();

    public static class CacheRecordAccessor
            implements MemoryBlockAccessor<CacheRecord> {

        private final EnterpriseSerializationService ss;
        private final MemoryManager memoryManager;
        private final Queue<CacheRecord> recordQ = new ArrayDeque<CacheRecord>(1024);
        private final Queue<OffHeapData> dataQ = new ArrayDeque<OffHeapData>(1024);

        public CacheRecordAccessor(EnterpriseSerializationService ss) {
            this.ss = ss;
            this.memoryManager = ss.getMemoryManager();
        }

        @Override
        public boolean isEqual(long address, CacheRecord value) {
            return isEqual(address, value.address());
        }

        @Override
        public boolean isEqual(long address1, long address2) {
            long valueAddress1 = UnsafeHelper.UNSAFE.getLong(address1 + CacheRecord.VALUE_OFFSET);
            long valueAddress2 = UnsafeHelper.UNSAFE.getLong(address2 + CacheRecord.VALUE_OFFSET);
            return OffHeapDataUtil.equals(valueAddress1, valueAddress2);
        }

        public CacheRecord newRecord() {
            CacheRecord record = recordQ.poll();
            if (record == null) {
                record = new CacheRecord();
            }
            return record;
        }

        @Override
        public CacheRecord read(long address) {
            if (address <= NULL_PTR) {
                throw new IllegalArgumentException("Illegal memory address: " + address);
            }
            CacheRecord record = newRecord();
            record.reset(address);
            return record;
        }

        @Override
        public void dispose(CacheRecord record) {
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

        OffHeapData readData(long valueAddress) {
            if (valueAddress <= NULL_PTR) {
                throw new IllegalArgumentException("Illegal memory address: " + valueAddress);
            }
            OffHeapData value = dataQ.poll();
            if (value == null) {
                value = new OffHeapData();
            }
            return value.reset(valueAddress);
        }

        void disposeValue(CacheRecord record) {
            long valueAddress = record.getValueAddress();
            if (valueAddress != NULL_PTR) {
                disposeData(valueAddress);
                record.setValueAddress(NULL_PTR);
            }
        }

        void disposeData(Data value) {
            if (value instanceof OffHeapData) {
                ss.disposeData(value);
                dataQ.offer((OffHeapData) value);
            }
        }

        void disposeData(long address) {
            OffHeapData data = readData(address);
            disposeData(data);
        }

        void enqueueRecord(CacheRecord record) {
            recordQ.offer(record.reset(NULL_PTR));
        }

        void enqueueData(OffHeapData data) {
            data.reset(NULL_PTR);
            dataQ.offer(data);
        }
    }
}
