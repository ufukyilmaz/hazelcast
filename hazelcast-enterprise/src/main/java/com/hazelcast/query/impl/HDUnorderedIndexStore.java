package com.hazelcast.query.impl;

import com.hazelcast.core.TypeConverter;
import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.StoreAdapter;
import com.hazelcast.query.Predicate;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.internal.util.ThreadUtil.assertRunningOnPartitionThread;
import static com.hazelcast.query.impl.AbstractIndex.NULL;

/**
 * Unordered index store for HD memory.
 * <p>
 * Contract:
 * - Whenever QueryableEntry is passed to it, expects the key & value to be NativeMemoryData
 * - Whenever Data is passed to it (removeInternal), expects it to be NativeMemoryData
 * - Never returns any native memory - all returning objects are on-heap (QueryableEntry and its fields).
 * - There is no read & write locking since it's accessed from a single partition-thread only
 */
class HDUnorderedIndexStore extends HDExpirableIndexStore {

    private final EnterpriseSerializationService ess;
    private final HDIndexHashMap<QueryableEntry> recordsWithNullValue;
    private final HDIndexNestedHashMap<QueryableEntry> records;

    HDUnorderedIndexStore(EnterpriseSerializationService ess, MemoryAllocator malloc,
                          MapEntryFactory<QueryableEntry> entryFactory, StoreAdapter partitionStoreAdapter) {
        // HD index does not use do any result set copying, thus we may pass NEVER here
        super(IndexCopyBehavior.NEVER, partitionStoreAdapter);
        assertRunningOnPartitionThread();
        this.ess = ess;

        this.recordsWithNullValue = new HDIndexHashMap<>(this, ess, malloc, entryFactory);
        this.records = new HDIndexNestedHashMap<>(this, ess, malloc, entryFactory);
    }

    @Override
    Object insertInternal(Comparable newValue, QueryableEntry entry) {
        assertRunningOnPartitionThread();
        if (newValue == NULL) {
            NativeMemoryData key = (NativeMemoryData) entry.getKeyData();
            MemoryBlock value = getValueToStore(entry);
            MemoryBlock oldValue = recordsWithNullValue.put(key, value);
            return getValueData(oldValue);
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
        clear();
        dispose();
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
        // Using a storage representation for arguments here to save on
        // conversions later.
        return canonicalizeScalarForStorage(value);
    }

    @Override
    public Comparable canonicalizeScalarForStorage(Comparable value) {
        // Assuming off-heap overhead of 13 bytes (12 for the NativeMemoryData
        // and 1 for the pooling manager) and allocation granularity by powers
        // of 2, there is no point in trying to represent a value in less than 2
        // bytes.

        if (!(value instanceof Number)) {
            return value;
        }

        Class clazz = value.getClass();

        Number number = (Number) value;

        if (Numbers.isDoubleRepresentable(clazz)) {
            double doubleValue = number.doubleValue();

            long longValue = number.longValue();
            if (Numbers.equalDoubles(doubleValue, (double) longValue)) {
                return canonicalizeLongRepresentable(longValue);
            } else if (clazz == Float.class) {
                return doubleValue;
            }
        } else if (Numbers.isLongRepresentable(clazz)) {
            return canonicalizeLongRepresentable(number.longValue());
        }

        return value;
    }

    @Override
    public void clear() {
        assertRunningOnPartitionThread();
        recordsWithNullValue.clear();
        records.clear();
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        return getRecordsInternal(canonicalize(value));
    }

    @Override
    public Set<QueryableEntry> getRecords(Set<Comparable> values) {
        assertRunningOnPartitionThread();
        Set<QueryableEntry> results = new HashSet<>();
        for (Comparable value : values) {
            // value is already canonicalized by the associated index
            results.addAll(getRecordsInternal(value));
        }
        return results;
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparison comparison, Comparable value) {
        assertRunningOnPartitionThread();
        Set<QueryableEntry> results = new HashSet<>();
        for (Data valueData : records.keySet()) {
            Comparable indexedValue = ess.toObject(valueData);
            boolean valid;
            int result = Comparables.compare(value, indexedValue);
            switch (comparison) {
                case LESS:
                    valid = result > 0;
                    break;
                case LESS_OR_EQUAL:
                    valid = result >= 0;
                    break;
                case GREATER:
                    valid = result < 0;
                    break;
                case GREATER_OR_EQUAL:
                    valid = result <= 0;
                    break;
                default:
                    throw new IllegalStateException("Unrecognized comparison: " + comparison);
            }
            if (valid) {
                results.addAll(records.get(indexedValue));
            }
        }
        return results;
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
        assertRunningOnPartitionThread();
        if (Comparables.compare(from, to) == 0) {
            if (!fromInclusive || !toInclusive) {
                return Collections.emptySet();
            }
            return records.get(canonicalize(from));
        }

        Set<QueryableEntry> results = new HashSet<>();
        int fromBound = fromInclusive ? 0 : +1;
        int toBound = toInclusive ? 0 : -1;
        for (Data valueData : records.keySet()) {
            Comparable value = ess.toObject(valueData);
            if (Comparables.compare(value, from) >= fromBound && Comparables.compare(value, to) <= toBound) {
                results.addAll(records.get(canonicalize(value)));
            }
        }
        return results;
    }

    private Set<QueryableEntry> getRecordsInternal(Comparable canonicalValue) {
        assertRunningOnPartitionThread();
        if (canonicalValue == NULL) {
            return recordsWithNullValue.entrySet();
        } else {
            return records.get(canonicalValue);
        }
    }

    private Object mapAttributeToEntry(Comparable attribute, QueryableEntry entry) {
        NativeMemoryData key = (NativeMemoryData) entry.getKeyData();
        MemoryBlock value = getValueToStore(entry);
        MemoryBlock oldValue = records.put(attribute, key, value);
        return getValueData(oldValue);
    }

    private Object removeMappingForAttribute(Comparable attribute, Data indexKey) {
        MemoryBlock oldValue = records.remove(attribute, (NativeMemoryData) indexKey);
        return getValueData(oldValue);
    }

    private void dispose() {
        assertRunningOnPartitionThread();
        records.dispose();
        recordsWithNullValue.dispose();
    }

    private Comparable canonicalize(Comparable value) {
        if (value instanceof CompositeValue) {
            Comparable[] components = ((CompositeValue) value).getComponents();
            for (int i = 0; i < components.length; ++i) {
                components[i] = canonicalizeScalarForStorage(components[i]);
            }
            return value;
        } else {
            return canonicalizeScalarForStorage(value);
        }
    }

    private static Comparable canonicalizeLongRepresentable(long value) {
        if (value == (long) (short) value) {
            return (short) value;
        } else {
            return value;
        }
    }

}
