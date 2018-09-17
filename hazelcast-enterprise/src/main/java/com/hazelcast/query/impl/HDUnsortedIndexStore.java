package com.hazelcast.query.impl;

import com.hazelcast.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.util.ThreadUtil.assertRunningOnPartitionThread;

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

    HDUnsortedIndexStore(EnterpriseSerializationService ess, MemoryAllocator malloc,
                         MapEntryFactory<QueryableEntry> entryFactory) {
        // HD index does not use do any result set copying, thus we may pass NEVER here
        super(IndexCopyBehavior.NEVER);
        assertRunningOnPartitionThread();
        this.ess = ess;

        this.recordsWithNullValue = new HDIndexHashMap<QueryableEntry>(ess, malloc, entryFactory);
        this.records = new HDIndexNestedHashMap<QueryableEntry>(ess, malloc, entryFactory);
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
        assertRunningOnPartitionThread();
        records.dispose();
        recordsWithNullValue.dispose();
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
        assertRunningOnPartitionThread();
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
        assertRunningOnPartitionThread();
        if (value instanceof IndexImpl.NullObject) {
            return recordsWithNullValue.entrySet();
        } else {
            return records.get(value);
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Set<Comparable> values) {
        assertRunningOnPartitionThread();
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

}
