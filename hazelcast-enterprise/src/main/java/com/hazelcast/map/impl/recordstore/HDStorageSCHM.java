package com.hazelcast.map.impl.recordstore;

import com.hazelcast.core.EntryView;
import com.hazelcast.internal.elastic.SlottableIterator;
import com.hazelcast.internal.elastic.map.SampleableElasticHashMap;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.iterator.MapKeysWithCursor;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * An extended {@link SampleableElasticHashMap} for Hi-Density
 * backed {@link IMap}.
 */
public class HDStorageSCHM extends SampleableElasticHashMap<HDRecord> {

    /**
     * Default capacity for a hash container.
     */
    public static final int DEFAULT_CAPACITY = 128;

    /**
     * Default load factor.
     */
    public static final float DEFAULT_LOAD_FACTOR = 0.6f;

    private final SerializationService serializationService;

    public HDStorageSCHM(HiDensityRecordProcessor<HDRecord> recordProcessor, SerializationService serializationService) {
        super(DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, recordProcessor);

        this.serializationService = serializationService;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <E extends SamplingEntry> E createSamplingEntry(int slot) {
        return (E) new LazyEntryViewFromRecord(slot, serializationService);
    }

    public MapKeysWithCursor fetchKeys(int tableIndex, int size) {
        SlottableIterator<Entry<Data, HDRecord>> iter = entryIter(tableIndex);
        List<Data> keys = new ArrayList<>(size);
        for (int i = 0; i < size && iter.hasNext(); i++) {
            Map.Entry<Data, HDRecord> entry = iter.next();
            Data key = entry.getKey();
            keys.add(memoryBlockProcessor.convertData(key, DataType.HEAP));
        }
        return new MapKeysWithCursor(keys, iter.getNextSlot());
    }

    public MapEntriesWithCursor fetchEntries(int tableIndex, int size) {
        SlottableIterator<Entry<Data, HDRecord>> iter = entryIter(tableIndex);
        List<Map.Entry<Data, Data>> entries = new ArrayList<>(size);
        for (int i = 0; i < size && iter.hasNext(); i++) {
            Map.Entry<Data, HDRecord> entry = iter.next();
            Data key = entry.getKey();
            Data value = entry.getValue().getValue();
            Data heapKeyData = memoryBlockProcessor.convertData(key, DataType.HEAP);
            Data heapValueData = memoryBlockProcessor.convertData(value, DataType.HEAP);
            entries.add(new AbstractMap.SimpleEntry<>(heapKeyData, heapValueData));
        }
        return new MapEntriesWithCursor(entries, iter.getNextSlot());
    }

    /**
     * Internally used {@link EntryView} implementation for sampling based eviction specific purposes.
     * <p>
     * Mainly:
     * <ul>
     * <li>Wraps a {@link Record} and reaches all {@link EntryView} specific info over it</li>
     * <li>Lazily de-serializes key and value</li>
     * </ul>
     *
     * @param <K> type of key
     * @param <V> type of value
     */
    public class LazyEntryViewFromRecord<K, V> extends SampleableElasticHashMap<HDRecord>.SamplingEntry
            implements EntryView<K, V> {

        private K key;
        private V value;
        private HDRecord record;

        private SerializationService serializationService;

        public LazyEntryViewFromRecord(int slot, SerializationService serializationService) {
            super(slot);
            this.record = super.getEntryValue();
            this.serializationService = serializationService;
        }

        LazyEntryViewFromRecord(int slot, SerializationService serializationService, HDRecord record) {
            super(slot);
            this.record = record;
            this.serializationService = serializationService;
        }

        @Override
        public K getKey() {
            assert Thread.currentThread() instanceof PartitionOperationThread;

            if (key == null) {
                key = serializationService.toObject(record.getKey());
            }
            return key;
        }

        @Override
        public V getValue() {
            assert Thread.currentThread() instanceof PartitionOperationThread;

            if (value == null) {
                this.value = serializationService.toObject(record.getValue());
            }
            return value;
        }

        @Override
        public long getCost() {
            return record.getCost();
        }

        @Override
        public long getCreationTime() {
            return record.getCreationTime();
        }

        @Override
        public long getExpirationTime() {
            return record.getExpirationTime();
        }

        @Override
        public long getHits() {
            return record.getHits();
        }

        @Override
        public long getLastAccessTime() {
            return record.getLastAccessTime();
        }

        @Override
        public long getLastStoredTime() {
            return record.getLastStoredTime();
        }

        @Override
        public long getLastUpdateTime() {
            return record.getLastUpdateTime();
        }

        @Override
        public long getVersion() {
            return record.getVersion();
        }

        @Override
        public long getTtl() {
            return record.getTtl();
        }

        @Override
        public long getMaxIdle() {
            return record.getMaxIdle();
        }

        public Record getRecord() {
            return record;
        }

        @Override
        @SuppressWarnings("checkstyle:cyclomaticcomplexity")
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (!(o instanceof EntryView)) {
                return false;
            }

            EntryView that = (EntryView) o;
            return getKey().equals(that.getKey())
                    && getValue().equals(that.getValue())
                    && getVersion() == that.getVersion()
                    && getCost() == that.getCost()
                    && getCreationTime() == that.getCreationTime()
                    && getExpirationTime() == that.getExpirationTime()
                    && getHits() == that.getHits()
                    && getLastAccessTime() == that.getLastAccessTime()
                    && getLastStoredTime() == that.getLastStoredTime()
                    && getLastUpdateTime() == that.getLastUpdateTime()
                    && getTtl() == that.getTtl();
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + getKey().hashCode();
            result = 31 * result + getValue().hashCode();

            long cost = getCost();
            long creationTime = getCreationTime();
            long expirationTime = getExpirationTime();
            long hits = getHits();
            long lastAccessTime = getLastAccessTime();
            long lastStoredTime = getLastStoredTime();
            long lastUpdateTime = getLastUpdateTime();
            long version = getVersion();
            long ttl = getTtl();

            result = 31 * result + (int) (cost ^ (cost >>> 32));
            result = 31 * result + (int) (creationTime ^ (creationTime >>> 32));
            result = 31 * result + (int) (expirationTime ^ (expirationTime >>> 32));
            result = 31 * result + (int) (hits ^ (hits >>> 32));
            result = 31 * result + (int) (lastAccessTime ^ (lastAccessTime >>> 32));
            result = 31 * result + (int) (lastStoredTime ^ (lastStoredTime >>> 32));
            result = 31 * result + (int) (lastUpdateTime ^ (lastUpdateTime >>> 32));
            result = 31 * result + (int) (version ^ (version >>> 32));
            result = 31 * result + (int) (ttl ^ (ttl >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "EntryView{key=" + getKey()
                    + ", value=" + getValue()
                    + ", cost=" + getCost()
                    + ", version=" + getVersion()
                    + ", creationTime=" + getCreationTime()
                    + ", expirationTime=" + getExpirationTime()
                    + ", hits=" + getHits()
                    + ", lastAccessTime=" + getLastAccessTime()
                    + ", lastStoredTime=" + getLastStoredTime()
                    + ", lastUpdateTime=" + getLastUpdateTime()
                    + ", ttl=" + getTtl()
                    + '}';
        }
    }
}
