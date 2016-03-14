package com.hazelcast.elastic.map;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.memory.MemoryBlockAccessor;
import com.hazelcast.internal.memory.MemoryBlockProcessor;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;

import static com.hazelcast.util.QuickMath.isPowerOfTwo;
import static com.hazelcast.util.QuickMath.nextPowerOfTwo;

/**
 * Variant of the {@link BinaryElasticHashMap} that allows quick random sampling of its entries.
 * @param <V> type of value.
 */
public class SampleableElasticHashMap<V extends MemoryBlock> extends BinaryElasticHashMap<V> {

    // Due to the constraints of JDK6 compatibility
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
        public V setValue(Object value) {
            throw new UnsupportedOperationException("Setting value is not supported");
        }

    }

    /**
     * Iterable sampling entry to preventing from extra object creation for iteration.
     *
     * NOTE: Assumed that it is not accessed by multiple threads. So there is synchronization.
     */
    @SuppressFBWarnings("PZ_DONT_REUSE_ENTRY_OBJECTS_IN_ITERATORS")
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
            return Collections.emptyList();
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
        private boolean reachedToEnd;
        private E currentSample;

        private LazySamplingEntryIterableIterator(int maxSampleCount) {
            this.maxSampleCount = maxSampleCount;
            this.end = capacity();
            assert isPowerOfTwo(end);
            this.segmentCount = Math.min(nextPowerOfTwo(maxSampleCount * 2), end);
            this.segmentSize = end / segmentCount;
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

        @SuppressWarnings("checkstyle:npathcomplexity")
        private void iterate() {
            if (returnedEntryCount >= maxSampleCount || reachedToEnd) {
                currentSample = null;
                return;
            }

            if (toRight) {
                // Iterate to right from current segment
                for (; passedSegmentCount < segmentCount;
                     passedSegmentCount++, currentSegmentNo = (currentSegmentNo + 1) % segmentCount) {
                    int segmentStart = currentSegmentNo * segmentSize;
                    int segmentEnd = segmentStart + segmentSize;
                    int ix = segmentStart + randomIndex;

                    // Find an allocated index to be sampled from current random index
                    while (ix < segmentEnd && !isValidForSampling(ix)) {
                        // Move to right in right-half of bucket
                        ix++;
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

            // Iterate to left from current segment
            for (; passedSegmentCount < segmentCount;
                 passedSegmentCount++, currentSegmentNo = (currentSegmentNo + 1) % segmentCount) {
                int segmentStart = currentSegmentNo * segmentSize;
                int ix = segmentStart + randomIndex - 1;

                // Find an allocated index to be sampled from current random index
                while (ix >= segmentStart && !isValidForSampling(ix)) {
                    // Move to left in left-half of bucket
                    ix--;
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

    protected boolean isValidForSampling(int slot) {
        return accessor.isAssigned(slot);
    }

}
