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

/**
 * Unsorted index store for HD memory.
 *
 * Contract:
 * - Whenever QueryableEntry is passed to it, expects the key & value to be NativeMemoryData
 * - Whenever Data is passed to it (removeIndexInternal), expects it to be NativeMemoryData
 * - Never returns any native memory - all returning objects are on-heap (QueryableEntry and its fields).
 * - There is no read & write locking since it's accessed from a single partition-thread only
 */
class HDUnsortedIndexStore extends BaseIndexStore {

    private final EnterpriseSerializationService ess;
    private final HDIndexHashMap<QueryableEntry> recordsWithNullValue;
    private final HDIndexNestedHashMap<QueryableEntry> records;

    HDUnsortedIndexStore(EnterpriseSerializationService ess, MemoryAllocator malloc) {
        this.ess = ess;
        this.recordsWithNullValue = new HDIndexHashMap<QueryableEntry>(ess, malloc, new OnHeapCachedQueryEntryFactory(ess));
        this.records = new HDIndexNestedHashMap<QueryableEntry>(ess, malloc, new OnHeapCachedQueryEntryFactory(ess));
    }

    @Override
    void newIndexInternal(Comparable newValue, QueryableEntry entry) {
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
        recordsWithNullValue.clear();
        records.clear();
    }

    private void dispose() {
        records.dispose();
        recordsWithNullValue.dispose();
    }

    @Override
    public void destroy() {
        clear();
        dispose();
    }

    @Override
    public Set<QueryableEntry> getSubRecordsBetween(Comparable from, Comparable to) {
        Set<QueryableEntry> results = new HashSet<QueryableEntry>();
        Comparable paramFrom = from;
        Comparable paramTo = to;
        int trend = paramFrom.compareTo(paramTo);
        if (trend == 0) {
            return records.get(paramFrom);
        }
        if (trend < 0) {
            Comparable oldFrom = paramFrom;
            paramFrom = to;
            paramTo = oldFrom;
        }
        for (Data valueData : records.keySet()) {
            Comparable value = ess.toObject(valueData);
            if (value.compareTo(paramFrom) <= 0 && value.compareTo(paramTo) >= 0) {
                results.addAll(records.get(value));
            }
        }
        return results;
    }

    @Override
    public Set<QueryableEntry> getSubRecords(ComparisonType comparisonType, Comparable searchedValue) {
        Set<QueryableEntry> results = new HashSet<QueryableEntry>();
        for (Data valueData : records.keySet()) {
            Comparable value = ess.toObject(valueData);
            boolean valid;
            int result = searchedValue.compareTo(value);
            switch (comparisonType) {
                case LESSER:
                    valid = result > 0;
                    break;
                case LESSER_EQUAL:
                    valid = result >= 0;
                    break;
                case GREATER:
                    valid = result < 0;
                    break;
                case GREATER_EQUAL:
                    valid = result <= 0;
                    break;
                case NOT_EQUAL:
                    valid = result != 0;
                    break;
                default:
                    throw new IllegalStateException("Unrecognized comparisonType: " + comparisonType);
            }
            if (valid) {
                results.addAll(records.get(value));
            }
        }
        return results;
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        if (value instanceof IndexImpl.NullObject) {
            return recordsWithNullValue.entrySet();
        } else {
            return records.get(value);
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Set<Comparable> values) {
        Set<QueryableEntry> results = new HashSet<QueryableEntry>();
        for (Comparable value : values) {
            results.addAll(getRecords(value));
        }
        return results;
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
