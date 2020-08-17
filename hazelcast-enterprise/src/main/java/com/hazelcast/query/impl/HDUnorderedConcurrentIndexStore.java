package com.hazelcast.query.impl;

import com.hazelcast.internal.bplustree.HashIndexBPlusTreeKeyComparator;
import com.hazelcast.internal.bplustree.HashIndexEntrySlotPayload;
import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
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
 * to be in either NativeMemoryData or HDRecord
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
        if (Comparables.compare(from, to) == 0) {
            if (!fromInclusive || !toInclusive) {
                return Collections.emptySet();
            }
            return getRecords(canonicalize(from));
        }

        Set<QueryableEntry> results = new HashSet<>();
        int fromBound = fromInclusive ? 0 : +1;
        int toBound = toInclusive ? 0 : -1;
        Iterator<Data> keys = records.getKeys();
        while (keys.hasNext()) {
            Comparable value = ess.toObject(keys.next());

            if (Comparables.compare(value, from) >= fromBound && Comparables.compare(value, to) <= toBound) {
                results.addAll(getRecords(canonicalize(value)));
            }
        }
        return results;
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparison comparison, Comparable value) {
        Set<QueryableEntry> results = new HashSet<>();
        Iterator<Data> keys = records.getKeys();
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
                results.addAll(getRecords(indexedValue));
            }
        }
        return results;
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
}
