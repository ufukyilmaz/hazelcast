package com.hazelcast.internal.elastic.map;

import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryBlockProcessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.util.ThreadLocalRandomProvider;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.memory.MemoryBlockAccessor;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;

import static com.hazelcast.internal.elastic.map.BehmSlotAccessor.rehash;
import static com.hazelcast.internal.util.HashUtil.computePerturbationValue;
import static com.hazelcast.internal.util.QuickMath.isPowerOfTwo;
import static com.hazelcast.internal.util.QuickMath.nextPowerOfTwo;

/**
 * Variant of the {@link BinaryElasticHashMap} that allows quick random sampling of its entries.
 *
 * @param <V> type of value
 */
public class SampleableElasticHashMap<V extends MemoryBlock> extends BinaryElasticHashMap<V> {

    public SampleableElasticHashMap(int initialCapacity,
                                    EnterpriseSerializationService serializationService,
                                    MemoryBlockAccessor<V> memoryBlockAccessor, MemoryAllocator malloc) {
        super(initialCapacity, serializationService, new NativeBehmSlotAccessorFactory(), memoryBlockAccessor, malloc);
    }

    public SampleableElasticHashMap(int initialCapacity, float loadFactor,
                                    EnterpriseSerializationService serializationService,
                                    MemoryBlockAccessor<V> memoryBlockAccessor, MemoryAllocator malloc) {
        super(initialCapacity, loadFactor, serializationService, new NativeBehmSlotAccessorFactory(),
                memoryBlockAccessor, malloc);
    }

    public SampleableElasticHashMap(int initialCapacity, MemoryBlockProcessor<V> memoryBlockProcessor) {
        super(initialCapacity, new NativeBehmSlotAccessorFactory(), memoryBlockProcessor);
    }

    public SampleableElasticHashMap(int initialCapacity, float loadFactor,
                                    MemoryBlockProcessor<V> memoryBlockProcessor) {
        super(initialCapacity, loadFactor, new NativeBehmSlotAccessorFactory(), memoryBlockProcessor);
    }

    /**
     * Entry to define keys and values for sampling.
     */
    public class SamplingEntry {

        private final int slot;

        protected SamplingEntry(final int slot) {
            this.slot = slot;
        }

        public NativeMemoryData getEntryKey() {
            return accessor.keyData(slot);
        }

        public V getEntryValue() {
            final long value = accessor.getValue(slot);
            return readV(value);
        }
    }

    /**
     * Iterable sampling entry to prevent from extra object creation for iteration.
     *
     * NOTE: Assumed that it is not accessed by multiple threads. So there is synchronization.
     */
    @SuppressFBWarnings("PZ_DONT_REUSE_ENTRY_OBJECTS_IN_ITERATORS")
    public class IterableSamplingEntry extends SamplingEntry
            implements Iterable<IterableSamplingEntry>, Iterator<IterableSamplingEntry> {

        private boolean iterated;

        public IterableSamplingEntry(final int slot) {
            super(slot);
        }

        @Override
        public Iterator<IterableSamplingEntry> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            return !iterated;
        }

        @Override
        public IterableSamplingEntry next() {
            if (iterated) {
                throw new NoSuchElementException();
            }
            iterated = true;
            return this;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Removing is supported");
        }
    }

    protected <E extends SamplingEntry> E createSamplingEntry(final int slot) {
        return (E) new SamplingEntry(slot);
    }

    public <E extends SamplingEntry> Iterable<E> getRandomSamples(int sampleCount) {
        if (sampleCount < 0) {
            throw new IllegalArgumentException("Sample count cannot be a negative value.");
        }
        if (sampleCount == 0 || size() == 0) {
            return Collections.emptyList();
        }

        return new LazySamplingEntryIterableIterator<>(sampleCount);
    }

    /**
     * Not thread safe
     */
    private final class LazySamplingEntryIterableIterator<E extends SamplingEntry> implements Iterable<E>, Iterator<E> {

        private static final int NOT_INITIALIZED = Integer.MIN_VALUE;

        private final int maxSampleCount;
        private final int end;
        private final int segmentCount;
        private final int segmentSize;
        private final int randomSegment;
        private final int randomIndex;
        private int currentSegmentNo;
        private boolean toRight;
        private int passedSegmentCount;
        private int returnedEntryCount;
        private int currentIndex;
        private boolean reachedToEnd;
        private E currentSample;

        private LazySamplingEntryIterableIterator(int maxSampleCount) {
            this.maxSampleCount = maxSampleCount;
            this.end = capacity();
            assert isPowerOfTwo(end);
            this.segmentCount = Math.min(nextPowerOfTwo(maxSampleCount * 2), end);
            this.segmentSize = end / segmentCount;
            final Random random = ThreadLocalRandomProvider.get();
            this.randomSegment = random.nextInt(segmentCount);
            this.randomIndex = random.nextInt(segmentSize);
            this.currentSegmentNo = randomSegment;
            this.passedSegmentCount = 0;
            this.toRight = true;
            this.currentIndex = NOT_INITIALIZED;
        }

        @Override
        public Iterator<E> iterator() {
            return this;
        }

        @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
        private void iterate() {
            if (returnedEntryCount >= maxSampleCount || reachedToEnd) {
                currentSample = null;
                return;
            }

            if (toRight) {
                // iterate to right from current segment
                for (; passedSegmentCount < segmentCount; nextSegment()) {
                    int segmentStart = currentSegmentNo * segmentSize;
                    int segmentEnd = segmentStart + segmentSize;
                    int ix = currentIndex == NOT_INITIALIZED
                            ? segmentStart + randomIndex
                            : currentIndex + 1;

                    // find an allocated index to be sampled from current random index
                    while (ix < segmentEnd && !isValidForSampling(ix)) {
                        // move to right in right-half of bucket
                        ix++;
                    }
                    if (ix < segmentEnd) {
                        currentSample = createSamplingEntry(ix);
                        currentIndex = ix;
                        returnedEntryCount++;
                        return;
                    }
                }
                // reset before iterating to left
                currentSegmentNo = randomSegment;
                passedSegmentCount = 0;
                currentIndex = NOT_INITIALIZED;
                toRight = false;
            }

            // iterate to left from current segment
            for (; passedSegmentCount < segmentCount; nextSegment()) {
                int segmentStart = currentSegmentNo * segmentSize;
                int ix = currentIndex == NOT_INITIALIZED
                        ? segmentStart + randomIndex - 1
                        : currentIndex - 1;

                // find an allocated index to be sampled from current random index
                while (ix >= segmentStart && !isValidForSampling(ix)) {
                    // move to left in left-half of bucket
                    ix--;
                }
                if (ix >= segmentStart) {
                    currentSample = createSamplingEntry(ix);
                    currentIndex = ix;
                    returnedEntryCount++;
                    return;
                }
            }

            reachedToEnd = true;
            currentSample = null;
        }

        @Override
        public boolean hasNext() {
            if (currentSample == null) {
                iterate();
            }
            return currentSample != null;
        }

        @Override
        public E next() {
            if (hasNext()) {
                E returnedItem = currentSample;
                currentSample = null;
                return returnedItem;
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Removing is not supported");
        }

        private void nextSegment() {
            passedSegmentCount++;
            // move to next segment
            currentSegmentNo = (currentSegmentNo + 1) % segmentCount;
            currentIndex = NOT_INITIALIZED;
        }

    }

    private boolean isValidForSampling(int slot) {
        return accessor.isAssigned(slot);
    }

    /**
     * Returns {@code true} if the given {@code key} has not been already observed
     * (or should have been observed) with the iteration state provided by the
     * {@code pointers}.
     *
     * @param keyHash  the hashcode of the key to check
     * @param pointers the iteration state
     * @return if the key should have already been observed by the iteration user
     */
    protected boolean hasNotBeenObserved(int keyHash, IterationPointer[] pointers) {
        if (pointers.length < 2) {
            // there was no resize yet so we most definitely haven't observed the entry
            return true;
        }

        // check only the pointers up to the last, we haven't observed it with the last pointer
        for (int i = 0; i < pointers.length - 1; i++) {
            IterationPointer iterationPointer = pointers[i];
            int tableCapacity = iterationPointer.getSize();
            int mask = tableCapacity - 1;
            int seenBaseSlot = iterationPointer.getIndex();

            int keySlot = rehash(keyHash, computePerturbationValue(tableCapacity));
            int keyBaseSlot = keySlot & mask;
            if (keyBaseSlot < seenBaseSlot) {
                // on a table with the given capacity, we have already consumed
                // entries with the base slot that the keyHash belongs to
                return false;
            }
        }
        // on none of the previous iteration pointers have we observed
        // the base slot of the provided keyHash
        return true;
    }
}
