package com.hazelcast.internal.elastic.tree;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.elastic.map.BehmSlotAccessorFactory;
import com.hazelcast.internal.elastic.map.BinaryElasticHashMap;
import com.hazelcast.internal.elastic.tree.impl.RedBlackTreeStore;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.memory.MemoryBlockAccessor;
import com.hazelcast.internal.nio.Disposable;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.query.impl.Comparables;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.hazelcast.internal.elastic.map.BinaryElasticHashMap.loadFromOffHeapHeader;
import static com.hazelcast.internal.serialization.DataType.HEAP;

/**
 * Nested map, so a map of maps, with two-tiers of keys.
 * First tier maps keys to segments. Each segment maps keys to values.
 * First tier is sorted by keys using the key comparator passed to the constructor.
 * Second tier is unsorted.
 * <p>
 * Uses OffHeapTreeStore Red-Black-Tree as the underlying store for the segments.
 * Each segment is stored under a segmentKey passed as Data (may be on-heap of off-heap Data).
 * OffHeapComparator is used to compare and sort the segments according to the comparison order returned by the comparator.
 * There is one segment per segmentKey.
 * <p>
 * Each segment uses BinaryElasticHashMap as the underlying segment storage.
 * <p>
 * Contract of each segment:
 * - Expects the key to be in the NativeMemoryData,
 *   Expects the value to be in MemoryBlock,
 * - Returns MemoryBlock and an actual type is customized via {@code memoryBlockAccessor},
 * - Never disposes any MemoryBlock passed to it,
 * <p>
 * Each method that returns MapEntry instances uses MapEntryFactory to create them.
 *
 * @param <T> type of the Map.Entry returned by the tree.
 * @param <V> type of the value return by the tree.
 */
public abstract class BinaryElasticNestedTreeMap<T extends Map.Entry, V extends MemoryBlock> {

    private static final long NULL_ADDRESS = 0L;

    protected final MapEntryFactory<T> mapEntryFactory;
    private final OffHeapTreeStore records;
    private final EnterpriseSerializationService ess;
    private final MemoryAllocator malloc;
    private final BehmSlotAccessorFactory behmSlotAccessorFactory;
    private final MemoryBlockAccessor memoryBlockAccessor;

    public BinaryElasticNestedTreeMap(EnterpriseSerializationService ess, MemoryAllocator malloc,
                                      OffHeapComparator keyComparator, MapEntryFactory<T> mapEntryFactory,
                                      BehmSlotAccessorFactory behmSlotAccessorFactory, MemoryBlockAccessor memoryBlockAccessor) {
        this.records = new RedBlackTreeStore(ess.getCurrentMemoryAllocator(), keyComparator);
        this.ess = ess;
        this.malloc = malloc;
        this.mapEntryFactory = mapEntryFactory != null ? mapEntryFactory : new DefaultMapEntryFactory<T>();
        this.behmSlotAccessorFactory = behmSlotAccessorFactory;
        this.memoryBlockAccessor = memoryBlockAccessor;
    }

    public BinaryElasticNestedTreeMap(EnterpriseSerializationService ess, MemoryAllocator malloc,
                                      OffHeapComparator keyComparator,
                                      BehmSlotAccessorFactory behmSlotAccessorFactory, MemoryBlockAccessor memoryBlockAccessor) {
        this(ess, malloc, keyComparator, null, behmSlotAccessorFactory, memoryBlockAccessor);
    }

    public V put(Data segmentKey, NativeMemoryData key, V value) {

        checkNotNullOrEmpty(segmentKey, "segmentKey can't be null or empty");
        checkNotNullOrEmpty(key, "key can't be null or empty");

        NativeMemoryData nativeSegmentKey = ess.toNativeData(segmentKey, malloc);
        BinaryElasticHashMap<V> map = null;
        MemoryBlock mapMemBlock = null;

        boolean deallocateNativeSegmentKey = true;
        boolean deallocateMapAndBlock = true;
        try {
            OffHeapTreeEntry entry = records.getEntry(nativeSegmentKey);
            if (entry == null) {
                map = new BinaryElasticHashMap<V>(ess, behmSlotAccessorFactory, memoryBlockAccessor,
                        malloc);
                mapMemBlock = map.storeHeaderOffHeap(malloc, NULL_ADDRESS);
                records.put(nativeSegmentKey, mapMemBlock);
                deallocateNativeSegmentKey = false;
            } else {
                mapMemBlock = entry.values().next();
                map = loadFromOffHeapHeader(ess, malloc, mapMemBlock.address(), behmSlotAccessorFactory, memoryBlockAccessor);
            }

            V oldValue = map.put(key, value);
            map.storeHeaderOffHeap(malloc, mapMemBlock.address());
            deallocateMapAndBlock = false;
            return oldValue;
        } finally {
            if (deallocateNativeSegmentKey) {
                dispose(nativeSegmentKey);
            }

            if (deallocateMapAndBlock) {
                dispose(map, mapMemBlock);
            }
        }
    }

    public V get(Data segmentKey, NativeMemoryData key) {

        checkNotNullOrEmpty(segmentKey, "segmentKey can't be null");
        checkNotNullOrEmpty(key, "key can't be null");

        NativeMemoryData nativeSegmentKey = null;
        try {
            nativeSegmentKey = ess.toNativeData(segmentKey, malloc);
            OffHeapTreeEntry entry = records.getEntry(nativeSegmentKey);
            if (entry == null) {
                return null;
            }

            MemoryBlock value = entry.values().next();
            if (value == null) {
                return null;
            }

            BinaryElasticHashMap<V> map = loadFromOffHeapHeader(ess, malloc, value.address(),
                    behmSlotAccessorFactory, memoryBlockAccessor);
            return map.get(key);
        } finally {
            dispose(nativeSegmentKey);
        }

    }

    public Set<T> get(Data segmentKey) {

        checkNotNullOrEmpty(segmentKey, "segmentKey can't be null or empty");

        NativeMemoryData nativeSegmentKey = null;
        try {
            nativeSegmentKey = ess.toNativeData(segmentKey, malloc);
            if (nativeSegmentKey.address() == NULL_ADDRESS) {
                return Collections.emptySet();
            }

            OffHeapTreeEntry entry = records.getEntry(nativeSegmentKey);
            if (entry == null) {
                return Collections.emptySet();
            }

            MemoryBlock value = entry.values().next();
            if (value == null) {
                return Collections.emptySet();
            }

            BinaryElasticHashMap<MemoryBlock> map = loadFromOffHeapHeader(ess, malloc, value.address(),
                    behmSlotAccessorFactory, memoryBlockAccessor);
            Set<T> result = new HashSet<T>(map.size());

            addEntries(result, map.entrySet());

            return result;
        } finally {
            dispose(nativeSegmentKey);
        }

    }

    protected abstract void addEntries(Set<T> result, Set<Map.Entry<Data, MemoryBlock>> entrySet);

    public V remove(Data segmentKey, NativeMemoryData key) {

        checkNotNullOrEmpty(segmentKey, "segmentKey can't be null or empty");
        checkNotNullOrEmpty(key, "key can't be null or empty");

        OffHeapTreeEntry entry = records.getEntry((HeapData) segmentKey);
        if (entry == null) {
            return null;
        }

        MemoryBlock blob = entry.values().next();
        if (blob == null) {
            return null;
        }

        BinaryElasticHashMap<V> map = loadFromOffHeapHeader(ess, malloc, blob.address(),
                behmSlotAccessorFactory, memoryBlockAccessor);
        V value = map.remove(key);
        MemoryBlock keyBlob = entry.getKey();

        if (map.isEmpty()) {
            try {
                records.remove(entry);
            } finally {
                dispose(keyBlob, blob, map);
            }
        } else {
            map.storeHeaderOffHeap(malloc, blob.address());
        }
        return value;
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity", "checkstyle:methodlength",
            "checkstyle:nestedifdepth", "checkstyle:returncount"})
    public Set<T> subMap(Data fromSegmentKey, boolean fromInclusive, Data toSegmentKey, boolean toInclusive) {

        EntryIterator iterator;
        NativeMemoryData nativeSegmentKey = null;
        try {
            if (fromSegmentKey != null) {
                nativeSegmentKey = ess.toNativeData(fromSegmentKey, malloc);

                OffHeapTreeEntry nearestMatch = records.searchEntry(nativeSegmentKey);
                if (nearestMatch == null) {
                    return Collections.emptySet();
                }

                iterator = new EntryIterator(nearestMatch);
                if (!iterator.hasNext()) {
                    return Collections.emptySet();
                }

                // this is the from-element search case. The search might have finished before the search element
                // let's say in case we're looking for 14, and the leaf was 13, so there was no greater element
                // to follow on the "greater" path
                Comparable fromSegment = ess.toObject(fromSegmentKey);
                while (true) {
                    iterator.next();
                    Comparable currentSegment = ess.toObject(iterator.getKey());
                    if (Comparables.compare(currentSegment, fromSegment) >= 0) {
                        break;
                    } else if (!iterator.hasNext()) {
                        return Collections.emptySet();
                    }
                }

            } else {
                // if null begin from the very beginning
                iterator = new EntryIterator();
                fromInclusive = true;
                if (!iterator.hasNext()) {
                    return Collections.emptySet();
                }
                iterator.next();
            }

            Comparable toSegment = null;
            Set<T> result = new HashSet<T>();
            while (true) {
                BinaryElasticHashMap<MemoryBlock> map = iterator.value;

                // has to be comparison
                boolean toKeyMatched = false;
                if (toSegmentKey != null) {
                    if (toSegment == null) {
                        toSegment = ess.toObject(toSegmentKey);
                    }
                    Comparable currentSegment = ess.toObject(iterator.key);
                    int comparisionResult = Comparables.compare(currentSegment, toSegment);
                    if (comparisionResult > 0) {
                        return result;
                    }
                    toKeyMatched = comparisionResult == 0;
                }

                if (!fromInclusive) {
                    if (toKeyMatched && !toInclusive) {
                        // in this case, we skip the first element, and we do not include the last, so no element returned
                        return result;
                    } else {
                        // in this case we just mark fromInclusive to true since we skipped one element not to evaluate
                        // this if again
                        fromInclusive = true;
                        // The equal call below may produce a deserialization, but
                        // only a single one per query.
                        if (equal(iterator.key, fromSegmentKey)) {
                            if (iterator.hasNext()) {
                                iterator.next();
                                continue;
                            } else {
                                return result;
                            }
                        }
                    }
                }

                if (toKeyMatched && !toInclusive) {
                    // here we skip the addition of the last element and finish the loop
                    return result;
                }

                addEntries(result, map.entrySet());
                if (toKeyMatched) {
                    // here we add the last element and finish the loop
                    return result;
                }
                if (iterator.hasNext()) {
                    iterator.next();
                } else {
                    return result;
                }
            }
        } finally {
            dispose(nativeSegmentKey);
        }
    }

    public Set<T> headMap(Data toSegmentKey, boolean inclusive) {
        return subMap(null, true, toSegmentKey, inclusive);
    }

    public Set<T> tailMap(Data fromSegmentKey, boolean inclusive) {
        if (isNullOrEmptyData(fromSegmentKey)) {
            return Collections.emptySet();
        }
        return subMap(fromSegmentKey, inclusive, null, true);
    }

    public Set<T> exceptMap(Data exceptSegmentKey) {
        EntryIterator iterator = new EntryIterator();
        Set<T> result = new HashSet<T>();
        while (iterator.hasNext()) {
            BinaryElasticHashMap<MemoryBlock> map = iterator.next();
            // The equal call below may produce a deserialization on every
            // iteration, but luckily enough exceptMap is not used by the query
            // engine currently: NotEqualPredicate is marked as unindexed, so
            // it's never hitting the indexes; basically, Comparison.NOT_EQUAL
            // is never used by any part of Hazelcast except tests.
            if (exceptSegmentKey != null && equal(exceptSegmentKey, iterator.getKey())) {
                continue;
            }
            addEntries(result, map.entrySet());
        }
        return result;
    }

    private boolean equal(Data lhs, Data rhs) {
        if (lhs.equals(rhs)) {
            return true;
        }

        Comparable lhsComparable = ess.toObject(lhs);
        Comparable rhsComparable = ess.toObject(rhs);

        if (lhsComparable == null || rhsComparable == null) {
            return lhsComparable == rhsComparable;
        }

        return Comparables.compare(lhsComparable, rhsComparable) == 0;
    }

    private class EntryIterator implements Iterator<BinaryElasticHashMap<MemoryBlock>> {

        private Iterator<OffHeapTreeEntry> entryIterator;
        private Iterator<MemoryBlock> valueIterator;

        private Data key;
        private BinaryElasticHashMap<MemoryBlock> value;

        EntryIterator(OffHeapTreeEntry entry) {
            entryIterator = records.entries(entry);
            advanceKeyIterator();
        }

        EntryIterator() {
            entryIterator = records.entries();
            advanceKeyIterator();
        }

        private void advanceKeyIterator() {
            if (entryIterator.hasNext()) {
                OffHeapTreeEntry entry = entryIterator.next();
                key = toHeapData(entry.getKey());
                valueIterator = entry.values();
            } else {
                key = null;
                value = null;
            }
        }

        private void advanceValueIterator() {
            if (valueIterator.hasNext()) {
                MemoryBlock valueBlob = valueIterator.next();
                value = loadFromOffHeapHeader(ess, malloc, valueBlob.address(), behmSlotAccessorFactory, memoryBlockAccessor);
            } else {
                value = null;
            }
        }

        @Override
        public boolean hasNext() {
            if (valueIterator == null) {
                return false;
            } else if (valueIterator.hasNext()) {
                return true;
            } else {
                advanceKeyIterator();
                return valueIterator.hasNext();
            }
        }

        @Override
        public BinaryElasticHashMap<MemoryBlock> next() {
            if (valueIterator == null) {
                throw new NoSuchElementException();
            } else if (valueIterator.hasNext()) {
                advanceValueIterator();
                return value;
            } else {
                advanceKeyIterator();
                if (valueIterator.hasNext()) {
                    advanceValueIterator();
                    return value;
                } else {
                    throw new NoSuchElementException();
                }
            }
        }

        public Data getKey() {
            return key;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    /**
     * Clears the map by removing and disposing all segments including key/value pairs stored.
     */
    public void clear() {
        Iterator<OffHeapTreeEntry> treeEntryIterator = records.entries();
        while (treeEntryIterator.hasNext()) {
            OffHeapTreeEntry entry = treeEntryIterator.next();
            MemoryBlock keyBlob = entry.getKey();
            MemoryBlock valueBlob = entry.values().next();

            BinaryElasticHashMap<NativeMemoryData> map = loadFromOffHeapHeader(ess, malloc, valueBlob.address(),
                    behmSlotAccessorFactory, memoryBlockAccessor);

            try {
                records.remove(entry);
            } finally {
                dispose(map, valueBlob, keyBlob);
            }
        }
    }

    public void dispose() {
        clear();
        records.dispose(false);
    }

    public long size() {
        long size = 0;
        EntryIterator iterator = new EntryIterator();
        while (iterator.hasNext()) {
            BinaryElasticHashMap<MemoryBlock> map = iterator.next();
            size += map.size();
        }
        return size;
    }

    private Data toHeapData(MemoryBlock blob) {
        NativeMemoryData nativeMemoryData = new NativeMemoryData(blob.address(), blob.size());
        return ess.toData(nativeMemoryData, HEAP);
    }

    private boolean isNullOrEmptyData(Data data) {
        return data == null || data.totalSize() == 0;
    }

    private void checkNotNullOrEmpty(Data data, String message) {
        if (isNullOrEmptyData(data)) {
            throw new IllegalArgumentException(message);
        }
    }

    private void dispose(Object... objects) {
        Exception caught = null;
        for (Object object : objects) {
            if (object == null) {
                continue;
            }

            try {
                if (object instanceof Disposable) {
                    ((Disposable) object).dispose();
                } else if (object instanceof NativeMemoryData) {
                    NativeMemoryDataUtil.dispose(ess, malloc, (NativeMemoryData) object);
                } else if (object instanceof MemoryBlock) {
                    NativeMemoryDataUtil.dispose(ess, malloc, (MemoryBlock) object);
                } else {
                    throw new IllegalStateException("Unidentifiable object, don't know how to dispose. May cause leaks.");
                }
            } catch (Exception exception) {
                caught = exception;
            }
        }

        if (caught != null) {
            throw new HazelcastException("Could not deallocate memory. There may be a native memory leak!", caught);
        }
    }

    private static class DefaultMapEntryFactory<T extends Map.Entry> implements MapEntryFactory<T> {

        @Override
        @SuppressWarnings("unchecked")
        public T create(Data key, Data value) {
            return (T) new AbstractMap.SimpleEntry(key, value);
        }

    }

}
