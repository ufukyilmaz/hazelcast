package com.hazelcast.query.impl;

import com.hazelcast.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.query.impl.getters.Extractors;

import java.util.HashSet;
import java.util.Set;

/**
 * We don't need read&write locking since it's accessed from a single partition-thread only
 */
class HDSortedIndexStore extends BaseIndexStore {

    private final HDIndexHashMap<QueryableEntry> recordsWithNullValue;
    private final HDIndexNestedTreeMap<QueryableEntry> records;

    HDSortedIndexStore(EnterpriseSerializationService ess, MemoryAllocator malloc) {
        this.recordsWithNullValue = new HDIndexHashMap<QueryableEntry>(ess, malloc, new CachedQueryEntryFactory(ess));
        this.records = new HDIndexNestedTreeMap<QueryableEntry>(ess, malloc, new CachedQueryEntryFactory(ess));
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

    @Override
    public Set<QueryableEntry> getSubRecordsBetween(Comparable from, Comparable to) {
        return records.subMap(from, true, to, true);
    }

    @Override
    public Set<QueryableEntry> getSubRecords(ComparisonType comparisonType, Comparable searchedValue) {
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

    private static class CachedQueryEntryFactory implements MapEntryFactory<QueryableEntry> {
        private final EnterpriseSerializationService ess;

        CachedQueryEntryFactory(EnterpriseSerializationService ess) {
            this.ess = ess;
        }

        @Override
        public CachedQueryEntry create(Data key, Data value) {
            return new CachedQueryEntry(ess, key, value, Extractors.empty());
        }
    }

}
