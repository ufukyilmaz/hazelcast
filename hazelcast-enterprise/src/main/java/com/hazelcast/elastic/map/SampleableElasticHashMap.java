package com.hazelcast.elastic.map;

import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.memory.MemoryBlockAccessor;
import com.hazelcast.memory.MemoryBlockProcessor;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.util.QuickMath;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;

public class SampleableElasticHashMap<V extends MemoryBlock> extends BinaryElasticHashMap<V> {

    // Because of JDK6 compatibility,
    // we cannot use "java.util.concurrent.ThreadLocalRandom" (valid for JDK7+ versions).
    private static final ThreadLocal<Random> THREAD_LOCAL_RANDOM =
            new ThreadLocal<Random>() {
                @Override
                protected Random initialValue() {
                    return new Random();
                }
            };

    public SampleableElasticHashMap(EnterpriseSerializationService serializationService,
            MemoryBlockAccessor<V> memoryBlockAccessor, MemoryAllocator malloc) {
        super(serializationService, memoryBlockAccessor, malloc);
    }

    public SampleableElasticHashMap(int initialCapacity,
            EnterpriseSerializationService serializationService,
            MemoryBlockAccessor<V> memoryBlockAccessor, MemoryAllocator malloc) {
        super(initialCapacity, serializationService, memoryBlockAccessor, malloc);
    }

    public SampleableElasticHashMap(int initialCapacity, float loadFactor,
            EnterpriseSerializationService serializationService,
            MemoryBlockAccessor<V> memoryBlockAccessor, MemoryAllocator malloc) {
        super(initialCapacity, loadFactor, serializationService, memoryBlockAccessor, malloc);
    }

    public SampleableElasticHashMap(int initialCapacity, MemoryBlockProcessor<V> memoryBlockProcessor) {
        super(initialCapacity, memoryBlockProcessor);
    }

    public SampleableElasticHashMap(int initialCapacity, float loadFactor,
            MemoryBlockProcessor<V> memoryBlockProcessor) {
        super(initialCapacity, loadFactor, memoryBlockProcessor);
    }

    /**
     * Entry to define keys and values for sampling.
     */
    public class SamplingEntry extends MapEntry {

        public SamplingEntry(final int slot) {
            super(slot);
        }

        @Override
        public V setValue(MemoryBlock value) {
            throw new UnsupportedOperationException("Setting value is not supported");
        }

    }

    /**
     * Iterable sampling entry to preventing from extra object creation for iteration.
     *
     * NOTE: Assumed that it is not accessed by multiple threads. So there is synchronization.
     */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings("PZ_DONT_REUSE_ENTRY_OBJECTS_IN_ITERATORS")
    public class IterableSamplingEntry
            extends SamplingEntry
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
            return Collections.EMPTY_LIST;
        }

        return new LazySamplingEntryIterableIterator(sampleCount);
    }

    /**
     * This class is implements both of "Iterable" and "Iterator" interfaces.
     * So we can use only one object (instead of two) both for "Iterable" and "Iterator" interfaces.
     *
     * NOTE: Assumed that it is not accessed by multiple threads. So there is synchronization.
     */
    private final class LazySamplingEntryIterableIterator<E extends SamplingEntry>
            implements Iterable<E>, Iterator<E> {

        private final int maxEntryCount;
        private final int end;
        private final int mask;
        private final int segmentCount;
        private final int segmentSize;
        private final int randomSegment;
        private final int randomIndex;
        private int currentSegmentNo;
        private boolean toRight;
        private int passedSegmentCount;
        private int returnedEntryCount;
        private boolean reachedToEnd;
        private E currentSample;

        private LazySamplingEntryIterableIterator(int maxEntryCount) {
            this.maxEntryCount = maxEntryCount;
            this.end = capacity();
            this.mask = end - 1;
            this.segmentCount = QuickMath.nextPowerOfTwo(maxEntryCount * 2);
            this.segmentSize = Math.max(capacity() / segmentCount, 1);
            final Random random = THREAD_LOCAL_RANDOM.get();
            this.randomSegment = random.nextInt(segmentCount);
            this.randomIndex = random.nextInt(segmentSize);
            this.currentSegmentNo = randomSegment;
            this.passedSegmentCount = 0;
            this.toRight = true;
        }

        @Override
        public Iterator<E> iterator() {
            return this;
        }

        private void iterate() {
            if (returnedEntryCount >= maxEntryCount || reachedToEnd) {
                currentSample = null;
                return;
            }

            if (toRight) {
                // Iterate to right from current segment
                for (; passedSegmentCount < segmentCount;
                     passedSegmentCount++, currentSegmentNo = (currentSegmentNo + 1) % segmentCount) {
                    int segmentStart = currentSegmentNo * segmentSize;
                    int segmentEnd = Math.min(segmentStart + segmentSize, end);
                    int ix = segmentStart + randomIndex;
                    // If randomly selected index points to outside of allocated storage, start from head of segment
                    if (ix >= segmentEnd) {
                        ix = segmentStart;
                    }
                    // Find an allocated index to be sampled from current random index
                    while (ix < segmentEnd && !isAssigned(ix)) {
                        ix = (ix + 1) & mask; // Move to right in right-half of bucket
                    }
                    if (ix < segmentEnd) {
                        currentSample = createSamplingEntry(ix);
                        passedSegmentCount++;
                        // Move to next segment
                        currentSegmentNo = (currentSegmentNo + 1) % segmentCount;
                        returnedEntryCount++;
                        return;
                    }
                }
                // Reset before iterating to left
                currentSegmentNo = randomSegment;
                passedSegmentCount = 0;
                toRight = false;
            }

            if (!toRight) {
                // Iterate to left from current segment
                for (; passedSegmentCount < segmentCount;
                     passedSegmentCount++, currentSegmentNo = (currentSegmentNo + 1) % segmentCount) {
                    int segmentStart = currentSegmentNo * segmentSize;
                    int segmentEnd = Math.min(segmentStart + segmentSize, end);
                    int ix = Math.max(segmentStart, segmentStart + randomIndex - 1);
                    // If randomly selected index points to outside of allocated storage, start from tail of segment
                    if (ix >= segmentEnd) {
                        ix = segmentEnd - 1;
                    }
                    // Find an allocated index to be sampled from current random index
                    while (ix >= segmentStart && !isAssigned(ix)) {
                        ix = (ix - 1) & mask; // Move to left in left-half of bucket
                    }
                    if (ix >= segmentStart) {
                        currentSample = createSamplingEntry(ix);
                        passedSegmentCount++;
                        // Move to next segment
                        currentSegmentNo = (currentSegmentNo + 1) % segmentCount;
                        returnedEntryCount++;
                        return;
                    }
                }
            }

            reachedToEnd = true;
            currentSample = null;
        }

        @Override
        public boolean hasNext() {
            iterate();
            return currentSample != null;
        }

        @Override
        public E next() {
            if (currentSample != null) {
                return currentSample;
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Removing is not supported");
        }

    }

}
