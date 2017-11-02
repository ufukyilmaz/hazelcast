package com.hazelcast.query.impl;

import com.hazelcast.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.query.impl.getters.Extractors;

import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.nio.serialization.DataType.HEAP;
import static com.hazelcast.util.ThreadUtil.assertRunningOnPartitionThread;

/**
 * Sorted index store for HD memory.
 *
 * Contract:
 * - Whenever QueryableEntry is passed to it, expects the key & value to be NativeMemoryData
 * - Whenever Data is passed to it (removeIndexInternal), expects it to be NativeMemoryData
 * - Never returns any native memory - all returning objects are on-heap (QueryableEntry and its fields).
 * - There is no read & write locking since it's accessed from a single partition-thread only
 */
class HDSortedIndexStore extends BaseIndexStore {

    private final HDIndexHashMap<QueryableEntry> recordsWithNullValue;
    private final HDIndexNestedTreeMap<QueryableEntry> records;

    HDSortedIndexStore(EnterpriseSerializationService ess, MemoryAllocator malloc) {
        // HD index does not use do any result set copying, thus we may pass NEVER here
        super(IndexCopyBehavior.NEVER);
        assertRunningOnPartitionThread();
        this.recordsWithNullValue = new HDIndexHashMap<QueryableEntry>(ess, malloc, new OnHeapCachedQueryEntryFactory(ess));
        this.records = new HDIndexNestedTreeMap<QueryableEntry>(ess, malloc, new OnHeapCachedQueryEntryFactory(ess));
    }

    @Override
    void newIndexInternal(Comparable newValue, QueryableEntry entry) {
        assertRunningOnPartitionThread();
        if (newValue instanceof IndexImpl.NullObject) {
            NativeMemoryData key = (NativeMemoryData) entry.getKeyData();
            NativeMemoryData value = (NativeMemoryData) entry.getValueData();
            recordsWithNullValue.put(key, value);
        } else {
            mapAttributeToEntry(newValue, entry);
        }
    }

    private void mapAttributeToEntry(Comparable attribute, QueryableEntry entry) {
        NativeMemoryData key = (NativeMemoryData) entry.getKeyData();
        NativeMemoryData value = (NativeMemoryData) entry.getValueData();
        records.put(attribute, key, value);
    }

    @Override
    void removeIndexInternal(Comparable oldValue, Data indexKey) {
        assertRunningOnPartitionThread();
        if (oldValue instanceof IndexImpl.NullObject) {
            recordsWithNullValue.remove(indexKey);
        } else {
            removeMappingForAttribute(oldValue, indexKey);
        }
    }

    private void removeMappingForAttribute(Comparable attribute, Data indexKey) {
        records.remove(attribute, (NativeMemoryData) indexKey);
    }

    @Override
    public void clear() {
        assertRunningOnPartitionThread();
        recordsWithNullValue.clear();
        records.clear();
    }

    private void dispose() {
        recordsWithNullValue.dispose();
        records.dispose();
    }

    @Override
    public void destroy() {
        assertRunningOnPartitionThread();
        clear();
        dispose();
    }

    @Override
    public Set<QueryableEntry> getSubRecordsBetween(Comparable from, Comparable to) {
        assertRunningOnPartitionThread();
        return records.subMap(from, true, to, true);
    }

    @Override
    public Set<QueryableEntry> getSubRecords(ComparisonType comparisonType, Comparable searchedValue) {
        assertRunningOnPartitionThread();
        Set<QueryableEntry> results;
        switch (comparisonType) {
            case LESSER:
                results = records.headMap(searchedValue, false);
                break;
            case LESSER_EQUAL:
                results = records.headMap(searchedValue, true);
                break;
            case GREATER:
                results = records.tailMap(searchedValue, false);
                break;
            case GREATER_EQUAL:
                results = records.tailMap(searchedValue, true);
                break;
            case NOT_EQUAL:
                results = records.exceptMap(searchedValue);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized comparisonType: " + comparisonType);
        }
        return results;
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        return doGetRecords(value);
    }

    @Override
    public Set<QueryableEntry> getRecords(Set<Comparable> values) {
        Set<QueryableEntry> results = new HashSet<QueryableEntry>();
        for (Comparable value : values) {
            results.addAll(doGetRecords(value));
        }
        return results;
    }

    private Set<QueryableEntry> doGetRecords(Comparable value) {
        assertRunningOnPartitionThread();
        if (value instanceof IndexImpl.NullObject) {
            return recordsWithNullValue.entrySet();
        } else {
            return records.get(value);
        }
    }

    @Override
    public String toString() {
        return "HDSortedIndexStore{"
                + "recordMap=" + records.hashCode()
                + '}';
    }

    private static class OnHeapCachedQueryEntryFactory implements MapEntryFactory<QueryableEntry> {
        private final EnterpriseSerializationService ess;

        OnHeapCachedQueryEntryFactory(EnterpriseSerializationService ess) {
            this.ess = ess;
        }

        @Override
        public CachedQueryEntry create(Data key, Data value) {
            Data heapData = toHeapData(key);
            Data heapValue = toHeapData(value);
            return new CachedQueryEntry(ess, heapData, heapValue, Extractors.empty());
        }

        private Data toHeapData(Data data) {
            if (data instanceof NativeMemoryData) {
                NativeMemoryData nativeMemoryData = (NativeMemoryData) data;
                if (nativeMemoryData.totalSize() == 0) {
                    return null;
                }
                return ess.toData(nativeMemoryData, HEAP);
            }
            return data;
        }

    }

}
