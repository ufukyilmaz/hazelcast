package com.hazelcast.query.impl;

import com.hazelcast.core.TypeConverter;
import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.query.Predicate;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.hazelcast.internal.util.ThreadUtil.assertRunningOnPartitionThread;
import static com.hazelcast.query.impl.AbstractIndex.NULL;

/**
 * Ordered index store for HD memory.
 * <p>
 * Contract:
 * - Whenever QueryableEntry is passed to it, expects the key to be NativeMemoryData and the value
 * to be in either NativeMemoryData
 * - Whenever Data is passed to it (removeInternal), expects it to be NativeMemoryData
 * - Never returns any native memory - all returning objects are on-heap (QueryableEntry and its fields).
 * - There is no read & write locking since it's accessed from a single partition-thread only
 */
@SuppressWarnings("rawtypes")
class HDOrderedIndexStore extends BaseSingleValueIndexStore {

    private final HDIndexHashMap<QueryableEntry> recordsWithNullValue;
    private final HDIndexNestedTreeMap<QueryableEntry> records;

    HDOrderedIndexStore(EnterpriseSerializationService ess,
                        MemoryAllocator malloc,
                        MapEntryFactory<QueryableEntry> entryFactory) {
        // HD index does not use do any result set copying, thus we may pass NEVER here
        super(IndexCopyBehavior.NEVER, false);
        assertRunningOnPartitionThread();
        this.recordsWithNullValue = new HDIndexHashMap<QueryableEntry>(ess, malloc, entryFactory);
        try {
            this.records = new HDIndexNestedTreeMap<QueryableEntry>(ess, malloc, entryFactory);
        } catch (NativeOutOfMemoryError e) {
            recordsWithNullValue.dispose();
            throw e;
        }
    }

    @Override
    Object insertInternal(Comparable newValue, QueryableEntry entry) {
        assertRunningOnPartitionThread();
        if (newValue == NULL) {
            NativeMemoryData key = (NativeMemoryData) entry.getKeyData();
            MemoryBlock value = ((NativeMemoryData) entry.getValueData());
            MemoryBlock oldValue = recordsWithNullValue.put(key, value);
            return oldValue;
        } else {
            return mapAttributeToEntry(newValue, entry);
        }
    }

    @Override
    Object removeInternal(Comparable value, Data recordKey) {
        assertRunningOnPartitionThread();
        if (value == NULL) {
            return recordsWithNullValue.remove(recordKey);
        } else {
            return removeMappingForAttribute(value, recordKey);
        }
    }

    @Override
    public void destroy() {
        assertRunningOnPartitionThread();
        try {
            clear();
        } finally {
            dispose();
        }
    }

    @Override
    public boolean isEvaluateOnly() {
        return false;
    }

    @Override
    public boolean canEvaluate(Class<? extends Predicate> predicateClass) {
        return false;
    }

    @Override
    public Set<QueryableEntry> evaluate(Predicate predicate, TypeConverter converter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Comparable canonicalizeQueryArgumentScalar(Comparable value) {
        // We still need to canonicalize query arguments for ordered indexes to
        // support InPredicate queries.
        return Comparables.canonicalizeForHashLookup(value);
    }

    @Override
    public Comparable canonicalizeScalarForStorage(Comparable value) {
        // Returning the original value since ordered indexes are not supporting
        // hash lookups on their stored values, so there is no need in providing
        // canonical representations.
        return value;
    }

    @Override
    public void clear() {
        assertRunningOnPartitionThread();
        recordsWithNullValue.clear();
        records.clear();
    }

    @Override
    public Iterator<QueryableEntry> getSqlRecordIterator(boolean descending) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<QueryableEntry> getSqlRecordIterator(Comparable value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<QueryableEntry> getSqlRecordIterator(Comparison comparison, Comparable value, boolean descending) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<QueryableEntry> getSqlRecordIterator(
        Comparable from,
        boolean fromInclusive,
        Comparable to,
        boolean toInclusive,
        boolean descending
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        return doGetRecords(value);
    }

    @Override
    public Set<QueryableEntry> getRecords(Set<Comparable> values) {
        Set<QueryableEntry> results = new HashSet<>();
        for (Comparable value : values) {
            results.addAll(doGetRecords(value));
        }
        return results;
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparison comparison, Comparable value) {
        assertRunningOnPartitionThread();
        Set<QueryableEntry> results;
        switch (comparison) {
            case LESS:
                results = records.headMap(value, false);
                break;
            case LESS_OR_EQUAL:
                results = records.headMap(value, true);
                break;
            case GREATER:
                results = records.tailMap(value, false);
                break;
            case GREATER_OR_EQUAL:
                results = records.tailMap(value, true);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized comparison: " + comparison);
        }
        return results;
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
        assertRunningOnPartitionThread();
        return records.subMap(from, fromInclusive, to, toInclusive);
    }

    private Object mapAttributeToEntry(Comparable attribute, QueryableEntry entry) {
        NativeMemoryData key = (NativeMemoryData) entry.getKeyData();
        MemoryBlock value = ((NativeMemoryData) entry.getValueData());
        MemoryBlock oldValue = records.put(attribute, key, value);
        return oldValue;
    }

    private Object removeMappingForAttribute(Comparable attribute, Data indexKey) {
        MemoryBlock oldValue = records.remove(attribute, (NativeMemoryData) indexKey);
        return oldValue;
    }

    private void dispose() {
        recordsWithNullValue.dispose();
        records.dispose();
    }

    private Set<QueryableEntry> doGetRecords(Comparable value) {
        assertRunningOnPartitionThread();
        if (value == NULL) {
            return recordsWithNullValue.entrySet();
        } else {
            return records.get(value);
        }
    }

}
