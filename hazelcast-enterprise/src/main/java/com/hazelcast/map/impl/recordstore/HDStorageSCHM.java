package com.hazelcast.map.impl.recordstore;

import com.hazelcast.core.EntryView;
import com.hazelcast.internal.elastic.map.SampleableElasticHashMap;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.iterator.MapKeysWithCursor;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.elastic.map.BehmSlotAccessor.rehash;
import static com.hazelcast.internal.util.HashUtil.computePerturbationValue;


/**
 * An extended {@link SampleableElasticHashMap} for Hi-Density
 * backed {@link com.hazelcast.map.IMap}.
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
        return (E) new LazyEvictableEntryView(slot, serializationService);
    }

    /**
     * Fetch minimally {@code size} keys from the {@code pointers} position.
     * The key is fetched on-heap.
     * The method may return less keys if iteration has completed.
     * <p>
     * NOTE: The implementation is free to return more than {@code size} items.
     * This can happen if we cannot easily resume from the last returned item
     * by receiving the {@code tableIndex} of the last item. The index can
     * represent a bucket with multiple items and in this case the returned
     * object will contain all items in that bucket, regardless if we exceed
     * the requested {@code size}.
     *
     * @param pointers the pointers defining the state of iteration
     * @param size     the minimal count of returned items, unless iteration has completed
     * @return fetched keys and the new iteration state
     */
    public MapKeysWithCursor fetchKeys(IterationPointer[] pointers, int size) {
        List<Data> keys = new ArrayList<>(size);
        IterationPointer[] newIterationPointers = fetchNext(pointers, size,
                (k, v) -> keys.add(memoryBlockProcessor.convertData(k, DataType.HEAP)));
        return new MapKeysWithCursor(keys, newIterationPointers);
    }


    /**
     * Fetch minimally {@code size} items from the {@code pointers} position.
     * Both the key and value are fetched on-heap.
     * <p>
     * NOTE: The implementation is free to return more than {@code size} items.
     * This can happen if we cannot easily resume from the last returned item
     * by receiving the {@code tableIndex} of the last item. The index can
     * represent a bucket with multiple items and in this case the returned
     * object will contain all items in that bucket, regardless if we exceed
     * the requested {@code size}.
     *
     * @param pointers the pointers defining the state of iteration
     * @param size     the minimal count of returned items
     * @return fetched entries and the new iteration state
     */
    public MapEntriesWithCursor fetchEntries(IterationPointer[] pointers, int size) {
        List<Map.Entry<Data, Data>> entries = new ArrayList<>(size);
        IterationPointer[] newIterationPointers = fetchNext(pointers, size, (k, v) -> {
            Data heapKeyData = memoryBlockProcessor.convertData(k, DataType.HEAP);
            Data heapValueData = memoryBlockProcessor.convertData(v, DataType.HEAP);
            entries.add(new SimpleEntry<>(heapKeyData, heapValueData));
        });
        return new MapEntriesWithCursor(entries, newIterationPointers);
    }

    /**
     * Fetches at least {@code size} keys from the given {@code pointers} and
     * invokes the {@code entryConsumer} for each key-value pair.
     *
     * @param pointers         the pointers defining the state where to begin iteration
     * @param size             Count of how many entries will be fetched
     * @param keyValueConsumer the consumer to call with fetched key-value pairs
     * @return the pointers defining the state where iteration has ended
     */
    private IterationPointer[] fetchNext(IterationPointer[] pointers,
                                         int size,
                                         BiConsumer<Data, Data> keyValueConsumer) {
        IterationPointer[] updatedPointers = checkPointers(pointers, capacity());
        IterationPointer lastPointer = updatedPointers[updatedPointers.length - 1];
        int currentBaseSlot = lastPointer.getIndex();
        int[] fetchedEntries = {0};

        while (fetchedEntries[0] < size && currentBaseSlot >= 0) {
            currentBaseSlot = fetchAllWithBaseSlot(currentBaseSlot, slot -> {
                long currentKey = accessor.getKey(slot);
                int currentKeyHashCode = NativeMemoryDataUtil.hashCode(currentKey);
                if (hasNotBeenObserved(currentKeyHashCode, updatedPointers)) {
                    long valueAddr = accessor.getValue(slot);
                    HDRecord record = readV(valueAddr);
                    NativeMemoryData keyData = accessor.keyData(slot);
                    keyValueConsumer.accept(keyData, record.getValue());
                    fetchedEntries[0]++;
                }
            });
            lastPointer.setIndex(currentBaseSlot);
        }
        // either fetched enough entries or there are no more entries to iterate over
        return updatedPointers;
    }

    /**
     * Returns {@code true} if the given {@code key} has not been already observed
     * (or should have been observed) with the iteration state provided by the
     * {@code pointers}.
     *
     * @param keyHash  the hashcode of the key to check
     * @param pointers the iteration state
     * @return if the key should have already been observed by the iteration user
     */
    private boolean hasNotBeenObserved(int keyHash, IterationPointer[] pointers) {
        if (pointers.length < 2) {
            // there was no resize yet so we most definitely haven't observed the entry
            return true;
        }

        // check only the pointers up to the last, we haven't observed it with the last pointer
        for (int i = 0; i < pointers.length - 1; i++) {
            IterationPointer iterationPointer = pointers[i];
            int tableCapacity = iterationPointer.getSize();
            int mask = tableCapacity - 1;
            int seenBaseSlot = iterationPointer.getIndex();

            int keySlot = rehash(keyHash, computePerturbationValue(tableCapacity));
            int keyBaseSlot = keySlot & mask;
            if (keyBaseSlot < seenBaseSlot) {
                // on a table with the given capacity, we have already consumed
                // entries with the base slot that the keyHash belongs to
                return false;
            }
        }
        // on none of the previous iteration pointers have we observed
        // the base slot of the provided keyHash
        return true;
    }

    /**
     * Checks the {@code pointers} to see if we need to restart iteration on the
     * current table and returns the updated pointers if necessary.
     *
     * @param pointers         the pointers defining the state of iteration
     * @param currentTableSize the current table size
     * @return the updated pointers, if necessary
     */
    private IterationPointer[] checkPointers(IterationPointer[] pointers, int currentTableSize) {
        IterationPointer lastPointer = pointers[pointers.length - 1];
        boolean iterationStarted = lastPointer.getSize() == -1;
        boolean tableResized = lastPointer.getSize() != currentTableSize;
        // clone pointers to avoid mutating given reference
        // add new pointer if resize happened during iteration
        int newLength = !iterationStarted && tableResized ? pointers.length + 1 : pointers.length;

        IterationPointer[] updatedPointers = new IterationPointer[newLength];
        for (int i = 0; i < pointers.length; i++) {
            updatedPointers[i] = new IterationPointer(pointers[i]);
        }

        // reset last pointer if we haven't started iteration or there was a resize
        if (iterationStarted || tableResized) {
            updatedPointers[updatedPointers.length - 1] = new IterationPointer(Integer.MAX_VALUE, currentTableSize);
        }
        return updatedPointers;
    }

    /**
     * Internally used {@link EntryView} implementation
     * for sampling based eviction specific purposes.
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
    public class LazyEvictableEntryView<K, V>
            extends SampleableElasticHashMap<HDRecord>.SamplingEntry implements EntryView<K, V> {

        private K key;
        private V value;
        private HDRecord record;
        private Data dataKey;

        private SerializationService serializationService;

        public LazyEvictableEntryView(int slot, SerializationService serializationService) {
            super(slot);
            this.dataKey = super.getEntryKey();
            this.record = super.getEntryValue();
            this.serializationService = serializationService;
        }

        LazyEvictableEntryView(int slot, SerializationService serializationService,
                               Data dataKey, HDRecord record) {
            super(slot);
            this.dataKey = dataKey;
            this.record = record;
            this.serializationService = serializationService;
        }

        public Data getDataKey() {
            return dataKey;
        }

        public Record getRecord() {
            return record;
        }

        private void ensureCallingFromPartitionOperationThread() {
            if (Thread.currentThread().getClass() != PartitionOperationThread.class) {
                throw new IllegalThreadStateException(Thread.currentThread() + " cannot access data!");
            }
        }

        @Override
        public K getKey() {
            ensureCallingFromPartitionOperationThread();

            if (key == null) {
                key = serializationService.toObject(dataKey);
            }
            return key;
        }

        @Override
        public V getValue() {
            ensureCallingFromPartitionOperationThread();

            if (value == null) {
                this.value = serializationService.toObject(record.getValue());
            }
            return value;
        }

        @Override
        public long getCost() {
            ensureCallingFromPartitionOperationThread();

            return record.getCost();
        }

        @Override
        public long getCreationTime() {
            ensureCallingFromPartitionOperationThread();

            return record.getCreationTime();
        }

        @Override
        public long getExpirationTime() {
            ensureCallingFromPartitionOperationThread();

            return record.getExpirationTime();
        }

        @Override
        public long getHits() {
            ensureCallingFromPartitionOperationThread();

            return record.getHits();
        }

        @Override
        public long getLastAccessTime() {
            ensureCallingFromPartitionOperationThread();

            return record.getLastAccessTime();
        }

        @Override
        public long getLastStoredTime() {
            ensureCallingFromPartitionOperationThread();

            return record.getLastStoredTime();
        }

        @Override
        public long getLastUpdateTime() {
            ensureCallingFromPartitionOperationThread();

            return record.getLastUpdateTime();
        }

        @Override
        public long getVersion() {
            ensureCallingFromPartitionOperationThread();

            return record.getVersion();
        }

        @Override
        public long getTtl() {
            ensureCallingFromPartitionOperationThread();

            return record.getTtl();
        }

        @Override
        public long getMaxIdle() {
            ensureCallingFromPartitionOperationThread();

            return record.getMaxIdle();
        }

        @Override
        @SuppressWarnings("checkstyle:cyclomaticcomplexity")
        public boolean equals(Object o) {
            ensureCallingFromPartitionOperationThread();

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
            ensureCallingFromPartitionOperationThread();

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
            ensureCallingFromPartitionOperationThread();

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
