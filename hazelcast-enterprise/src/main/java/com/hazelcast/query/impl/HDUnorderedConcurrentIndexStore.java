package com.hazelcast.query.impl;

import com.hazelcast.internal.bplustree.HashIndexBPlusTreeKeyComparator;
import com.hazelcast.internal.bplustree.HashIndexEntrySlotPayload;
import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.hazelcast.query.impl.HDUnorderedIndexStore.canonicalize;
import static com.hazelcast.query.impl.HDUnorderedIndexStore.canonicalizeScalarForStorage0;

/**
 * Unordered and concurrent index store for HD memory based on B+tree.
 * <p>
 * The store doesn't compare index keys using Comparable.compareTo(..) but instead
 * compares the hash value of the index keys and if the hashes are equal compares the
 * serialized index keys byte-by-byte.
 * <p>
 * The hash value of the index key is stored in the entry slot payload and takes extra 8 bytes.
 * <p>
 * Contract:
 * - Whenever QueryableEntry is passed to it, expects the key to be NativeMemoryData and the value
 * to be in either NativeMemoryData
 * - Whenever Data is passed to it (removeInternal), expects it to be NativeMemoryData
 * - Iterator never returns any native memory - all returning objects are on-heap (QueryableEntry and its fields).
 * - The index operations are thread-safe and can be accessed from multiple threads concurrently
 */
public class HDUnorderedConcurrentIndexStore extends HDBaseConcurrentIndexStore {
    private final EnterpriseSerializationService ess;

    HDUnorderedConcurrentIndexStore(IndexCopyBehavior copyBehavior,
                                    EnterpriseSerializationService ess,
                                    MemoryAllocator keyAllocator,
                                    MemoryAllocator indexAllocator,
                                    MapEntryFactory<QueryableEntry> entryFactory,
                                    int nodeSize) {
        super(copyBehavior,
                ess,
                keyAllocator,
                indexAllocator,
                new HashIndexBPlusTreeKeyComparator(ess),
                entryFactory,
                nodeSize,
                new HashIndexEntrySlotPayload());
        this.ess = ess;
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
        Iterator<QueryableEntry> it = getSqlRecordIterator(canonicalize(from), fromInclusive, canonicalize(to), toInclusive);
        return buildResultSet(it);
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparison comparison, Comparable value) {
        Iterator<QueryableEntry> it = getSqlRecordIterator(comparison, canonicalize(value));
        return buildResultSet(it);
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        return super.getRecords(canonicalize(value));
    }

    @Override
    public Comparable canonicalizeScalarForStorage(Comparable value) {
        return canonicalizeScalarForStorage0(value);
    }

    @Override
    public Comparable canonicalizeQueryArgumentScalar(Comparable value) {
        // Using a storage representation for arguments here to save on
        // conversions later.
        return canonicalizeScalarForStorage(value);
    }

    @Override
    public Iterator<QueryableEntry> getSqlRecordIterator(Comparable value) {
        return super.getSqlRecordIterator(canonicalize(value));
    }

    @Override
    public Iterator<QueryableEntry> getSqlRecordIterator(Comparison comparison, Comparable value) {
        return new ValueComparisonIterator(comparison, canonicalize(value));
    }

    @Override
    public Iterator<QueryableEntry> getSqlRecordIterator(
            Comparable from,
            boolean fromInclusive,
            Comparable to,
            boolean toInclusive
    ) {
        if (Comparables.compare(from, to) == 0) {
            if (!fromInclusive || !toInclusive) {
                return Collections.emptyIterator();
            }
            return getSqlRecordIterator(canonicalize(from));
        }
        return new KeyRangeIterator(canonicalize(from), fromInclusive, canonicalize(to), toInclusive);
    }

    private class KeyRangeIterator implements Iterator<QueryableEntry> {
        private final Comparable from;
        private final Comparable to;
        private final int fromBound;
        private final int toBound;
        private final Iterator<Data> keys;
        private Iterator<QueryableEntry> currentIterator;

        KeyRangeIterator(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
            this.from = from;
            this.to = to;
            fromBound = fromInclusive ? 0 : +1;
            toBound = toInclusive ? 0 : -1;
            keys = records.getKeys();
        }

        @Override
        public boolean hasNext() {
            outer:
            while (true) {
                if (currentIterator == null) {
                    while (keys.hasNext()) {
                        Comparable value = ess.toObject(keys.next());
                        if (Comparables.compare(value, from) >= fromBound && Comparables.compare(value, to) <= toBound) {
                            currentIterator = getSqlRecordIterator(canonicalize(value));
                            continue outer;
                        }
                    }
                    return false;
                } else {
                    if (currentIterator.hasNext()) {
                        return true;
                    } else {
                        currentIterator = null;
                        continue;
                    }
                }
            }
        }

        @Override
        public QueryableEntry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return currentIterator.next();
        }
    }

    private class ValueComparisonIterator implements Iterator<QueryableEntry> {
        private final Comparison comparison;
        private final Comparable value;
        private final Iterator<Data> keys;
        private Iterator<QueryableEntry> currentIterator;

        ValueComparisonIterator(Comparison comparison, Comparable value) {
            this.comparison = comparison;
            this.value = value;
            keys = records.getKeys();
        }

        @Override
        public boolean hasNext() {
            outer:
            while (true) {
                if (currentIterator == null) {
                    while (keys.hasNext()) {
                        Comparable indexedValue = ess.toObject(keys.next());
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
                            currentIterator = getSqlRecordIterator(indexedValue);
                            continue outer;
                        }
                    }
                    return false;
                } else {
                    if (currentIterator.hasNext()) {
                        return true;
                    } else {
                        currentIterator = null;
                        continue;
                    }
                }
            }
        }

        @Override
        public QueryableEntry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return currentIterator.next();
        }
    }
}
