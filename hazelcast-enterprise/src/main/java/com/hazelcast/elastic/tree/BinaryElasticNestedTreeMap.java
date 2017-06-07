package com.hazelcast.elastic.tree;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.elastic.map.BinaryElasticHashMap;
import com.hazelcast.elastic.map.NativeMemoryDataAccessor;
import com.hazelcast.elastic.tree.iterator.OffHeapKeyIterator;
import com.hazelcast.elastic.tree.iterator.value.OffHeapValueIterator;
import com.hazelcast.elastic.tree.sorted.OffHeapKeyValueRedBlackTreeStorage;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.hazelcast.elastic.map.BinaryElasticHashMap.HEADER_LENGTH_IN_BYTES;
import static com.hazelcast.nio.serialization.DataType.HEAP;

/**
 * Expectes Key(HeapData) and Value(HeapData)
 * Takes care of converting to NATIVE format;
 *
 * @param <T> type of the Map.Entry returned by the tree.
 */
public class BinaryElasticNestedTreeMap<T extends Map.Entry> {

    private static final long NULL_ADDRESS = 0L;

    private final OffHeapKeyValueRedBlackTreeStorage records;

    private final EnterpriseSerializationService ess;
    private final MemoryAllocator malloc;
    private final MapEntryFactory<T> mapEntryFactory;

    public BinaryElasticNestedTreeMap(EnterpriseSerializationService ess, MemoryAllocator malloc,
                                      OffHeapComparator keyComparator, MapEntryFactory<T> mapEntryFactory) {
        this.records = new OffHeapKeyValueRedBlackTreeStorage(ess.getCurrentMemoryAllocator(), keyComparator);
        this.ess = ess;
        this.malloc = malloc;
        this.mapEntryFactory = mapEntryFactory != null ? mapEntryFactory : new DefaultMapEntryFactory<T>();
    }

    public BinaryElasticNestedTreeMap(EnterpriseSerializationService ess, MemoryAllocator malloc,
                                      OffHeapComparator keyComparator) {
        this(ess, malloc, keyComparator, null);
    }

    public NativeMemoryData put(Data segmentKey, NativeMemoryData key, NativeMemoryData value) {

        checkNotNullOrEmpty(segmentKey, "segmentKey can't be null or empty");
        checkNotNullOrEmpty(key, "key can't be null or empty");

        NativeMemoryData nativeSegmentKey = ess.toNativeData(segmentKey, malloc);
        long segmentKeyPointer = nativeSegmentKey.address();
        long segmentKeyWrittenSize = nativeSegmentKey.totalSize();
        long segmentKeyAllocatedSize = nativeSegmentKey.size();
        boolean deallocateNativeSegmentKey = true;
        try {
            BinaryElasticHashMap<NativeMemoryData> map;
            long mapHeaderPointer;
            long keyEntryAddress = records.getKeyEntry(segmentKeyPointer, segmentKeyWrittenSize, segmentKeyAllocatedSize, false);
            if (keyEntryAddress == NULL_ADDRESS) {
                map = new BinaryElasticHashMap<NativeMemoryData>(ess, new NativeMemoryDataAccessor(ess), malloc);
                mapHeaderPointer = map.storeHeaderOffHeap(malloc, NULL_ADDRESS).address();

                keyEntryAddress = records.put(
                        segmentKeyPointer, segmentKeyWrittenSize, segmentKeyAllocatedSize,
                        mapHeaderPointer, HEADER_LENGTH_IN_BYTES, HEADER_LENGTH_IN_BYTES
                );
                assert keyEntryAddress != NULL_ADDRESS;

                if (segmentKeyPointer == records.getKeyAddress(keyEntryAddress)) {
                    // is allocated for the first time
                    deallocateNativeSegmentKey = false;
                }
            } else {
                long valueEntryAddress = records.getValueEntryAddress(keyEntryAddress);
                mapHeaderPointer = records.getValueAddress(valueEntryAddress);
                map = BinaryElasticHashMap.loadFromOffHeapHeader(ess, malloc, mapHeaderPointer);
            }

            if (value == null) {
                value = new NativeMemoryData();
            }
            NativeMemoryData oldValue = map.put(key, value);
            map.storeHeaderOffHeap(malloc, mapHeaderPointer);
            return oldValue;
        } finally {
            if (deallocateNativeSegmentKey) {
                dispose(nativeSegmentKey);
            }
        }
    }

    public NativeMemoryData get(Data segmentKey, NativeMemoryData key) {

        checkNotNullOrEmpty(segmentKey, "segmentKey can't be null");
        checkNotNullOrEmpty(key, "key can't be null");

        NativeMemoryData nativeSegmentKey = null;
        try {
            nativeSegmentKey = ess.toNativeData(segmentKey, malloc);

            long segmentKeyPointer = nativeSegmentKey.address();
            long segmentKeyWrittenSize = nativeSegmentKey.totalSize();
            long segmentKeyAllocatedSize = nativeSegmentKey.size();

            long keyEntryAddress = records.getKeyEntry(segmentKeyPointer, segmentKeyWrittenSize, segmentKeyAllocatedSize, false);
            if (keyEntryAddress == NULL_ADDRESS) {
                return null;
            }
            long valueEntryAddress = records.getValueEntryAddress(keyEntryAddress);
            if (valueEntryAddress == NULL_ADDRESS) {
                return null;
            }

            long valuePointer = records.getValueAddress(valueEntryAddress);
            if (valuePointer == NULL_ADDRESS) {
                return null;
            }

            BinaryElasticHashMap<NativeMemoryData> map = BinaryElasticHashMap.loadFromOffHeapHeader(ess, malloc, valuePointer);
            return map.get(key);
        } finally {
            dispose(nativeSegmentKey);
        }

    }

    @SuppressWarnings("unchecked")
    public Set<T> get(Data segmentKey) {

        checkNotNullOrEmpty(segmentKey, "segmentKey can't be null or empty");

        NativeMemoryData nativeSegmentKey = null;
        try {
            nativeSegmentKey = ess.toNativeData(segmentKey, malloc);

            long segmentKeyPointer = nativeSegmentKey.address();
            long segmentKeyWrittenSize = nativeSegmentKey.totalSize();
            long segmentKeyAllocatedSize = nativeSegmentKey.size();
            if (segmentKeyPointer == NULL_ADDRESS) {
                return Collections.EMPTY_SET;
            }

            long keyEntryAddress = records.getKeyEntry(segmentKeyPointer, segmentKeyWrittenSize, segmentKeyAllocatedSize, false);
            if (keyEntryAddress == NULL_ADDRESS) {
                return Collections.EMPTY_SET;
            }
            long valueEntryAddress = records.getValueEntryAddress(keyEntryAddress);
            // not strictly necessary (by convention)
            if (valueEntryAddress == NULL_ADDRESS) {
                return Collections.EMPTY_SET;
            }
            long valuePointer = records.getValueAddress(valueEntryAddress);
            // not strictly necessary (by convention)
            if (valuePointer == NULL_ADDRESS) {
                return Collections.EMPTY_SET;
            }

            BinaryElasticHashMap<NativeMemoryData> map = BinaryElasticHashMap.loadFromOffHeapHeader(ess, malloc, valuePointer);
            Set<T> result = new HashSet<T>(map.size());

            for (Map.Entry<Data, NativeMemoryData> entry : map.entrySet()) {
                result.add(mapEntryFactory.create(
                        toHeapData((NativeMemoryData) entry.getKey()),
                        toHeapData(entry.getValue())));
            }

            return result;
        } finally {
            dispose(nativeSegmentKey);
        }

    }

    public NativeMemoryData remove(Data segmentKey, NativeMemoryData key) {

        checkNotNullOrEmpty(segmentKey, "segmentKey can't be null or empty");
        checkNotNullOrEmpty(key, "key can't be null or empty");

        NativeMemoryData nativeSegmentKey = null;
        try {
            nativeSegmentKey = ess.toNativeData(segmentKey, malloc);
            long segmentKeyPointer = nativeSegmentKey.address();
            long segmentKeyWrittenSize = nativeSegmentKey.totalSize();
            long segmentKeyAllocatedSize = nativeSegmentKey.size();

            long keyEntryAddress = records.getKeyEntry(segmentKeyPointer, segmentKeyWrittenSize, segmentKeyAllocatedSize, false);
            if (keyEntryAddress == NULL_ADDRESS) {
                return null;
            }
            long valueEntryAddress = records.getValueEntryAddress(keyEntryAddress);
            if (valueEntryAddress == NULL_ADDRESS) {
                return null;
            }

            long valuePointer = records.getValueAddress(valueEntryAddress);
            BinaryElasticHashMap<NativeMemoryData> map = BinaryElasticHashMap.loadFromOffHeapHeader(ess, malloc, valuePointer);
            NativeMemoryData value = map.remove(key);

            if (map.isEmpty()) {
                // TODO tkountis
                // this is the desired version when the RBT remove() method is ready
                //
                // map.dispose(); // disposing the BEHM map
                // records.remove(nativeSegmentKey.address()); // removing RBT entry
                // ess.disposeData(new NativeMemoryData().reset(valuePointer)); // removing the RBT node "value"

                // TODO tkountis
                // it should also deallocate the internal nativeSegmentKey (not the one allocated here)
                // but the one that has been stored internally when the first segmentKey was used

                // TODO remove this: this is the work-around version till the above is ready
                map.storeHeaderOffHeap(malloc, valuePointer);
            } else {
                map.storeHeaderOffHeap(malloc, valuePointer);
            }
            return value;
        } finally {
            dispose(nativeSegmentKey);
        }
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity", "checkstyle:methodlength",
            "checkstyle:nestedifdepth"})
    public Set<T> subMap(Data fromSegmentKey, boolean fromInclusive, Data toSegmentKey, boolean toInclusive) {

        EntryIterator iterator;
        NativeMemoryData nativeSegmentKey = null;
        try {
            if (fromSegmentKey != null) {
                nativeSegmentKey = ess.toNativeData(fromSegmentKey, malloc);
                long segmentKeyPointer = nativeSegmentKey.address();
                long segmentKeyWrittenSize = nativeSegmentKey.totalSize();
                long segmentKeyAllocatedSize = nativeSegmentKey.size();

                long keyEntryAddress = records.searchKeyEntry(segmentKeyPointer, segmentKeyWrittenSize, segmentKeyAllocatedSize);
                if (keyEntryAddress == NULL_ADDRESS) {
                    return Collections.EMPTY_SET;
                }
                iterator = new EntryIterator(keyEntryAddress);
                if (!iterator.hasNext()) {
                    return Collections.EMPTY_SET;
                }

                // this is the from-element search case. The search might have finished before the search element
                // let's say in case we're looking for 14, and the leaf was 13, so there was no greated element
                // to follow on the "greater" path
                while (true) {
                    iterator.next();
                    Comparable fromSegment = ess.toObject(fromSegmentKey);
                    Comparable currentSegment = ess.toObject(iterator.getKey());
                    if (currentSegment.compareTo(fromSegment) >= 0) {
                        break;
                    } else if (!iterator.hasNext()) {
                        return Collections.EMPTY_SET;
                    }
                }

            } else {
                // if null begin from the very beginning
                iterator = new EntryIterator();
                fromInclusive = true;
                if (!iterator.hasNext()) {
                    return Collections.EMPTY_SET;
                }
                iterator.next();
            }

            Comparable toSegment = null;
            Set<T> result = new HashSet<T>();
            while (true) {
                BinaryElasticHashMap<NativeMemoryData> map = iterator.value;

                // has to be comparison
                boolean toKeyMatched = false;
                if (toSegmentKey != null) {
                    if (toSegment == null) {
                        toSegment = ess.toObject(toSegmentKey);
                    }
                    Comparable currentSegment = ess.toObject(iterator.key);
                    int comparisionResult = currentSegment.compareTo(toSegment);
                    if (comparisionResult > 0) {
                        return result;
                    }
                    toKeyMatched = comparisionResult == 0;
                }

                if (!fromInclusive) {
                    if (toKeyMatched && !toInclusive) {
                        // in this case, we skip the first element, and we do not include the last, so no element returned
                        break;
                    } else {
                        // in this case we just mark fromInclusive to true since we skipped one element not to evaluate
                        // this if again
                        fromInclusive = true;
                        if (iterator.key.equals(fromSegmentKey)) {
                            if (iterator.hasNext()) {
                                iterator.next();
                                continue;
                            } else {
                                break;
                            }
                        }
                    }
                }

                if (toKeyMatched && !toInclusive) {
                    // here we skip the addition of the last element and finish the loop
                    break;
                }

                for (Map.Entry<Data, NativeMemoryData> entry : map.entrySet()) {
                    result.add(mapEntryFactory.create(
                            toHeapData((NativeMemoryData) entry.getKey()),
                            toHeapData(entry.getValue())));
                }
                if (toKeyMatched) {
                    // here we add the last element and finish the loop
                    break;
                }
                if (iterator.hasNext()) {
                    iterator.next();
                } else {
                    break;
                }
            }
            return result;
        } finally {
            dispose(nativeSegmentKey);
        }
    }

    public Set<T> headMap(Data toSegmentKey, boolean inclusive) {
        return subMap(null, true, toSegmentKey, inclusive);
    }

    public Set<T> tailMap(Data fromSegmentKey, boolean inclusive) {
        if (isNullOrEmptyData(fromSegmentKey)) {
            return Collections.EMPTY_SET;
        }
        return subMap(fromSegmentKey, inclusive, null, true);
    }

    public Set<T> exceptMap(Data exceptSegmentKey) {
        EntryIterator iterator = new EntryIterator();
        Set<T> result = new HashSet<T>();
        while (iterator.hasNext()) {
            BinaryElasticHashMap<NativeMemoryData> map = iterator.next();
            if (exceptSegmentKey != null && exceptSegmentKey.equals(iterator.getKey())) {
                continue;
            }
            for (Map.Entry<Data, NativeMemoryData> entry : map.entrySet()) {
                result.add(mapEntryFactory.create(
                        toHeapData((NativeMemoryData) entry.getKey()),
                        toHeapData(entry.getValue())));
            }
        }
        return result;
    }

    private class EntryIterator implements Iterator<BinaryElasticHashMap<NativeMemoryData>> {

        private OffHeapKeyIterator keyIterator;
        private OffHeapValueIterator valueIterator;

        private Data key;
        private BinaryElasticHashMap<NativeMemoryData> value;

        EntryIterator(long keyEntryAddress) {
            keyIterator = records.keyIterator(keyEntryAddress);
            advanceKeyIterator();
        }

        EntryIterator() {
            keyIterator = records.keyIterator();
            advanceKeyIterator();
        }

        private void advanceKeyIterator() {
            if (keyIterator.hasNext()) {
                long keyEntryPointer = keyIterator.next();
                if (keyEntryPointer == NULL_ADDRESS) {
                    key = null;
                    valueIterator = null;
                } else {
                    key = toHeapData(records.getKeyAddress(keyEntryPointer));
                    valueIterator = records.valueIterator(keyEntryPointer);
                }
            }
        }

        private void advanceValueIterator() throws IOException {
            long valueEntryPointer = valueIterator.next();
            if (valueEntryPointer == NULL_ADDRESS) {
                value = null;
            } else {
                long valueAddress = records.getValueAddress(valueEntryPointer);
                value = BinaryElasticHashMap.loadFromOffHeapHeader(ess, malloc, valueAddress);
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
        public BinaryElasticHashMap<NativeMemoryData> next() {
            try {
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
            } catch (IOException e) {
                throw new HazelcastException(e);
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

    public void clear() {
        // TODO tkountis (this is just a remporary solution)
        EntryIterator iterator = new EntryIterator();
        while (iterator.hasNext()) {
            BinaryElasticHashMap<NativeMemoryData> map = iterator.next();
            map.clear();
            map.dispose();
        }

        // TODO tkountis
        // Here it should deallocate all segment keys along with all BEHM maps (like example above)
        // It should also remove and deallocate all the keys and also all the internal RBT structures, leaving the
        // tree with a root address and nothing else allocated.
    }

    public void dispose() {
        records.dispose();
    }

    public long size() {
        long size = 0;
        EntryIterator iterator = new EntryIterator();
        while (iterator.hasNext()) {
            BinaryElasticHashMap<NativeMemoryData> map = iterator.next();
            size += map.size();
        }
        return size;
    }

    private Data toHeapData(long address) {
        NativeMemoryData nativeMemoryData = new NativeMemoryData().reset(address);
        return ess.toData(nativeMemoryData, HEAP);
    }

    private Data toHeapData(NativeMemoryData nativeMemoryData) {
        if (nativeMemoryData != null && nativeMemoryData.totalSize() == 0) {
            return null;
        }
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

    private void dispose(NativeMemoryData... nativeData) {
        NativeMemoryDataUtil.dispose(ess, malloc, nativeData);
    }

    private static class DefaultMapEntryFactory<T extends Map.Entry> implements MapEntryFactory<T> {
        @Override
        @SuppressWarnings("unchecked")
        public T create(Data key, Data value) {
            return (T) new AbstractMap.SimpleEntry(key, value);
        }
    }

}
