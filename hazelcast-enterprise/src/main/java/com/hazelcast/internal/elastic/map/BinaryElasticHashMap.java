package com.hazelcast.internal.elastic.map;

import com.hazelcast.internal.elastic.SlottableIterator;
import com.hazelcast.internal.memory.GlobalMemoryAccessor;
import com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.memory.MemoryBlockAccessor;
import com.hazelcast.internal.memory.MemoryBlockProcessor;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.hashslot.impl.CapacityUtil;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.Data;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

import static com.hazelcast.internal.elastic.map.BehmSlotAccessor.rehash;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.internal.serialization.impl.NativeMemoryData.NATIVE_MEMORY_DATA_OVERHEAD;
import static com.hazelcast.internal.util.HashUtil.computePerturbationValue;
import static com.hazelcast.internal.util.hashslot.impl.CapacityUtil.DEFAULT_LOAD_FACTOR;
import static com.hazelcast.internal.util.hashslot.impl.CapacityUtil.MIN_CAPACITY;
import static com.hazelcast.internal.util.hashslot.impl.CapacityUtil.nextCapacity;
import static com.hazelcast.internal.util.hashslot.impl.CapacityUtil.roundCapacity;

/**
 * A hash map of {@code Data} to {@code MemoryBlock}, implemented using open
 * addressing with linear probing for collision resolution.
 * <p>
 * The internal buffer of this implementation is
 * always allocated to the nearest higher size that is a power of two. When
 * the capacity exceeds the given load factor, the buffer size is doubled.
 *
 * @param <V> the type of memory block used as value
 */
@SuppressWarnings("checkstyle:methodcount")
public class BinaryElasticHashMap<V extends MemoryBlock> implements ElasticMap<Data, V> {

    /**
     * Header length when header stored off-heap for zero heap usage
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int HEADER_LENGTH_IN_BYTES = 36;

    protected final MemoryBlockProcessor<V> memoryBlockProcessor;

    protected BehmSlotAccessor accessor;
    private final BehmSlotAccessorFactory accessorFactory;

    /**
     * Number of allocated slots.
     */
    private int allocatedSlotCount;

    /**
     * Cached number of assigned slots in {@link #allocatedSlotCount}.
     */
    private int assignedSlotCount;

    /**
     * The load factor for this map (fraction of allocated slots
     * before the buffers must be rehashed or reallocated).
     */
    private final float loadFactor;

    /**
     * Resize buffers when {@link #assignedSlotCount} hits this value.
     */
    private int resizeAt;

    /**
     * We perturb hashed values with the array size to avoid problems with
     * nearly-sorted-by-hash values on iterations.
     *
     * @see "http://issues.carrot2.org/browse/HPPC-80"
     */
    private int perturbation;

    private final MemoryAllocator malloc;

    private Random random;

    /**
     * Used to provide fail fast behaviour in case of concurrent modification during iteration
     */
    private int modCount;

    /**
     * Creates a hash map with the default capacity of {@value CapacityUtil#DEFAULT_CAPACITY},
     * load factor of {@value CapacityUtil#DEFAULT_LOAD_FACTOR}.
     */
    public BinaryElasticHashMap(EnterpriseSerializationService serializationService,
                                BehmSlotAccessorFactory behmSlotAccessorFactory,
                                MemoryBlockAccessor<V> memoryBlockAccessor, MemoryAllocator malloc) {
        // checkstyle complains that CapacityUtil is an unused import so we use it once here
        this(CapacityUtil.DEFAULT_CAPACITY, serializationService, behmSlotAccessorFactory, memoryBlockAccessor, malloc);
    }

    /**
     * Creates a hash map with the given initial capacity, default load factor of
     * {@value CapacityUtil#DEFAULT_LOAD_FACTOR}.
     *
     * @param initialCapacity initial capacity (greater than zero and automatically
     *                        rounded to the next power of two)
     */
    public BinaryElasticHashMap(int initialCapacity, EnterpriseSerializationService serializationService,
                                BehmSlotAccessorFactory behmSlotAccessorFactory,
                                MemoryBlockAccessor<V> memoryBlockAccessor, MemoryAllocator malloc) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR, serializationService, behmSlotAccessorFactory,
                memoryBlockAccessor, malloc);
    }

    /**
     * Creates a hash map with the given initial capacity,
     * load factor.
     *
     * @param initialCapacity initial capacity (greater than zero and automatically
     *                        rounded to the next power of two)
     * @param loadFactor      the load factor (greater than zero and smaller than 1)
     */
    public BinaryElasticHashMap(int initialCapacity, float loadFactor,
                                EnterpriseSerializationService serializationService,
                                BehmSlotAccessorFactory behmSlotAccessorFactory,
                                MemoryBlockAccessor<V> memoryBlockAccessor, MemoryAllocator malloc) {
        this(initialCapacity, loadFactor, behmSlotAccessorFactory,
                new BehmMemoryBlockProcessor<V>(serializationService, memoryBlockAccessor, malloc));
    }

    /**
     * Creates a hash map with the given initial capacity,
     * load factor.
     *
     * @param initialCapacity initial capacity (greater than zero and automatically
     *                        rounded to the next power of two)
     */
    public BinaryElasticHashMap(int initialCapacity, BehmSlotAccessorFactory behmSlotAccessorFactory,
                                MemoryBlockProcessor<V> memoryBlockProcessor) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR, behmSlotAccessorFactory, memoryBlockProcessor);
    }

    /**
     * Creates a hash map with the given initial capacity,
     * load factor.
     *
     * @param initialCapacity initial capacity (greater than zero and automatically
     *                        rounded to the next power of two)
     * @param loadFactor      the load factor (greater than zero and smaller than 1)
     */
    public BinaryElasticHashMap(int initialCapacity, float loadFactor,
                                BehmSlotAccessorFactory behmSlotAccessorFactory,
                                MemoryBlockProcessor<V> memoryBlockProcessor) {
        initialCapacity = Math.max(initialCapacity, MIN_CAPACITY);

        assert initialCapacity > 0
                : "Initial capacity must be between (0, " + Integer.MAX_VALUE + "].";
        assert loadFactor > 0 && loadFactor <= 1
                : "Load factor must be between (0, 1].";

        this.loadFactor = loadFactor;
        this.memoryBlockProcessor = memoryBlockProcessor;
        this.malloc = memoryBlockProcessor.unwrapMemoryAllocator();
        this.accessorFactory = behmSlotAccessorFactory;

        allocateBuffer(roundCapacity(initialCapacity));
    }

    @SuppressWarnings("checkstyle:parameternumber")
    private BinaryElasticHashMap(int allocatedSlotCount, int assignedSlotCount, float loadFactor, int resizeAt,
                                 int perturbation, long baseAddress, long size, MemoryBlockAccessor<V> memoryBlockAccessor,
                                 MemoryAllocator malloc, EnterpriseSerializationService ess,
                                 BehmSlotAccessorFactory behmSlotAccessorFactory) {
        this.allocatedSlotCount = allocatedSlotCount;
        this.assignedSlotCount = assignedSlotCount;
        this.loadFactor = loadFactor;
        this.resizeAt = resizeAt;
        this.perturbation = perturbation;
        this.malloc = malloc;
        this.memoryBlockProcessor = new BehmMemoryBlockProcessor<V>(ess, memoryBlockAccessor, malloc);
        this.accessorFactory = behmSlotAccessorFactory;
        this.accessor = accessorFactory.create(malloc, baseAddress, size);
    }

    @SuppressWarnings("checkstyle:parameternumber")
    private BinaryElasticHashMap(int allocatedSlotCount, int assignedSlotCount, float loadFactor, int resizeAt,
                                 int perturbation, long baseAddress, long size,
                                 MemoryAllocator malloc, EnterpriseSerializationService ess,
                                 BehmSlotAccessorFactory behmSlotAccessorFactory,
                                 MemoryBlockAccessor memoryBlockAccessor) {
        this(allocatedSlotCount, assignedSlotCount, loadFactor, resizeAt, perturbation, baseAddress, size,
                memoryBlockAccessor, malloc, ess, behmSlotAccessorFactory);
    }

    private Random getRandom() {
        if (random == null) {
            random = new Random();
        }
        return this.random;
    }

    /**
     * Allocates internal buffer for a given capacity.
     *
     * @param capacity new capacity (must be a power of two)
     */
    private void allocateBuffer(int capacity) {
        try {
            accessor = accessorFactory.create(malloc, 0L, 0L).allocate(capacity);
        } catch (NativeOutOfMemoryError e) {
            throw onOome(e);
        }
        allocatedSlotCount = capacity;
        resizeAt = Math.max(2, (int) Math.ceil(capacity * loadFactor)) - 1;
        perturbation = computePerturbationValue(capacity);
    }

    protected NativeOutOfMemoryError onOome(NativeOutOfMemoryError e) {
        return e;
    }

    @Override
    public V put(Data key, V value) {
        ensureMemory();
        assert assignedSlotCount < allocatedSlotCount;

        final int mask = allocatedSlotCount - 1;
        int slot = rehash(key, perturbation) & mask;
        while (accessor.isAssigned(slot)) {
            long keyAddr = accessor.getKey(slot);
            if (NativeMemoryDataUtil.equals(keyAddr, key)) {
                final long oldValue = accessor.getValue(slot);
                accessor.setValue(slot, value);
                return readV(oldValue);
            }
            slot = (slot + 1) & mask;
        }

        NativeMemoryData memKey = (NativeMemoryData) memoryBlockProcessor.convertData(key, DataType.NATIVE);

        assert memKey.address() != NULL_ADDRESS : "Null key!";

        // check if we need to grow. If so, reallocate new data, fill in the last element
        // and rehash
        if (assignedSlotCount == resizeAt) {
            try {
                expandAndPut(memKey.address(), value, slot);
            } catch (Throwable error) {
                // if they are not same, this means that the key is converted to native memory data at here
                // so, it must be disposed at here
                if (memKey != key) {
                    memoryBlockProcessor.disposeData(memKey);
                }
                throw ExceptionUtil.rethrow(error);
            }

        } else {
            assignedSlotCount++;
            accessor.setKey(slot, memKey.address());
            accessor.setValue(slot, value);
        }

        modCount++;
        return null;
    }

    @Override
    public boolean set(Data key, V value) {
        V old = put(key, value);
        if (old != null && old.address() != value.address()) {
            memoryBlockProcessor.dispose(old);
        }
        return old == null;
    }

    @Override
    public V putIfAbsent(Data key, V value) {
        V current = get(key);
        if (current == null) {
            set(key, value);
            return null;
        }
        return current;
    }

    @Override
    public void putAll(Map<? extends Data, ? extends V> map) {
        for (Entry<? extends Data, ? extends V> entry : map.entrySet()) {
            set(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public boolean replace(Data key, V oldValue, V newValue) {
        ensureMemory();

        final int mask = allocatedSlotCount - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (accessor.isAssigned(slot)) {
            long slotKey = accessor.getKey(slot);
            if (NativeMemoryDataUtil.equals(slotKey, key)) {
                long current = accessor.getValue(slot);
                if (memoryBlockProcessor.isEqual(current, oldValue)) {
                    accessor.setValue(slot, newValue);
                    if (current != NULL_ADDRESS) {
                        memoryBlockProcessor.dispose(current);
                    }
                    return true;
                }
                return false;
            }
            slot = (slot + 1) & mask;
            if (slot == wrappedAround) {
                break;
            }
        }
        return false;
    }

    @Override
    public V replace(Data key, V value) {
        ensureMemory();
        final int mask = allocatedSlotCount - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (accessor.isAssigned(slot)) {
            long slotKey = accessor.getKey(slot);
            if (NativeMemoryDataUtil.equals(slotKey, key)) {
                long current = accessor.getValue(slot);
                accessor.setValue(slot, value);
                return readV(current);
            }
            slot = (slot + 1) & mask;
            if (slot == wrappedAround) {
                break;
            }
        }
        return null;
    }

    /**
     * Expands the internal storage buffers (capacity) and rehash.
     */
    private void expandAndPut(long pendingKey, V pendingValue, int freeSlot) {
        ensureMemory();
        assert assignedSlotCount == resizeAt;
        assert !accessor.isAssigned(freeSlot);

        final BehmSlotAccessor oldAccessor = accessorFactory.create(accessor);
        final int oldAllocated = allocatedSlotCount;

        // try to allocate new buffer first. If we OOM, it'll be now without
        // leaving the data structure in an inconsistent state
        allocateBuffer(nextCapacity(allocatedSlotCount));

        // we have succeeded at allocating new data so insert the pending key/value at
        // the free slot in the temporary arrays before rehashing
        assignedSlotCount++;
        oldAccessor.setKey(freeSlot, pendingKey);
        oldAccessor.setValue(freeSlot, pendingValue);

        // rehash all stored keys into the new buffers
        final int mask = allocatedSlotCount - 1;
        for (int slot = oldAllocated; --slot >= 0; ) {
            if (oldAccessor.isAssigned(slot)) {
                long key = oldAccessor.getKey(slot);
                long value = oldAccessor.getValue(slot);

                int newSlot = rehash(NativeMemoryDataUtil.hashCode(key), perturbation) & mask;
                while (accessor.isAssigned(newSlot)) {
                    newSlot = (newSlot + 1) & mask;
                }

                accessor.setKey(newSlot, key);
                accessor.setValue(newSlot, value);
            }
        }
        oldAccessor.delete();
    }

    @Override
    public V remove(Object k) {
        ensureMemory();
        Data key = (Data) k;
        final int mask = allocatedSlotCount - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (accessor.isAssigned(slot)) {
            long slotKey = accessor.getKey(slot);
            if (NativeMemoryDataUtil.equals(slotKey, key)) {
                assignedSlotCount--;
                long v = accessor.getValue(slot);
                if (key instanceof HeapData
                        || (key instanceof NativeMemoryData && ((NativeMemoryData) key).address() != slotKey)
                ) {
                    memoryBlockProcessor.disposeData(accessor.keyData(slot));
                }
                shiftConflictingKeys(slot);
                ++modCount;
                return readV(v);
            }
            slot = (slot + 1) & mask;
            if (slot == wrappedAround) {
                break;
            }
        }

        return null;
    }

    @Override
    public boolean delete(Data key) {
        V value = remove(key);
        if (value != null) {
            memoryBlockProcessor.dispose(value);
        }
        return value != null;
    }

    @Override
    public boolean remove(final Object k, final Object v) {
        ensureMemory();
        Data key = (Data) k;
        V value = (V) v;

        final int mask = allocatedSlotCount - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (accessor.isAssigned(slot)) {
            long keyAddress = accessor.getKey(slot);
            if (NativeMemoryDataUtil.equals(keyAddress, key)) {
                long current = accessor.getValue(slot);
                if (memoryBlockProcessor.isEqual(current, value)) {
                    assignedSlotCount--;
                    if (key instanceof HeapData
                            || (key instanceof NativeMemoryData && ((NativeMemoryData) key).address() != keyAddress)
                    ) {
                        memoryBlockProcessor.disposeData(accessor.keyData(slot));
                    }
                    if (value.address() != current) {
                        memoryBlockProcessor.dispose(current);
                    }
                    shiftConflictingKeys(slot);
                    return true;
                }
                return false;
            }
            slot = (slot + 1) & mask;
            if (slot == wrappedAround) {
                break;
            }
        }

        return false;
    }

    /**
     * Shifts all the slot-conflicting keys allocated to (and including) {@code slot}.
     */
    private void shiftConflictingKeys(int slotCurr) {
        final int mask = allocatedSlotCount - 1;
        int slotPrev;
        while (true) {
            slotPrev = slotCurr;
            slotCurr = (slotCurr + 1) & mask;

            while (accessor.isAssigned(slotCurr)) {
                int slotOther = rehash(NativeMemoryDataUtil.hashCode(accessor.getKey(slotCurr)), perturbation) & mask;

                if (slotPrev <= slotCurr) {
                    // We're on the right of the original slot.
                    if (slotPrev >= slotOther || slotOther > slotCurr) {
                        break;
                    }
                } else {
                    // we've wrapped around
                    if (slotPrev >= slotOther && slotOther > slotCurr) {
                        break;
                    }
                }
                slotCurr = (slotCurr + 1) & mask;
            }

            if (!accessor.isAssigned(slotCurr)) {
                break;
            }

            // shift key/value pair
            accessor.setKey(slotPrev, accessor.getKey(slotCurr));
            accessor.setValue(slotPrev, accessor.getValue(slotCurr));
        }

        accessor.setKey(slotPrev, 0L);
        accessor.setValue(slotPrev, 0L);
    }

    @Override
    public V get(Object k) {
        return get0(k, false);
    }

    /**
     * @param k must be {@code instanceof NativeMemoryData}
     * @return same as {@link #get(Object)}, but with the additional constraint that the argument
     * must refer to exactly the same blob (at the same address) as the one stored by the map
     */
    public V getIfSameKey(Object k) {
        return get0(k, true);
    }

    private V get0(Object k, boolean onlyIfSame) {
        ensureMemory();

        Data key = (Data) k;
        final int mask = allocatedSlotCount - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (accessor.isAssigned(slot)) {
            long slotKeyAddr = accessor.getKey(slot);
            if (onlyIfSame ? same(slotKeyAddr, (NativeMemoryData) key) : NativeMemoryDataUtil.equals(slotKeyAddr, key)) {
                long value = accessor.getValue(slot);
                return readV(value);
            }
            slot = (slot + 1) & mask;
            if (slot == wrappedAround) {
                break;
            }
        }
        return null;
    }

    private boolean same(long addrOfKey1, NativeMemoryData key2) {
        return addrOfKey1 == key2.address();
    }

    public long getNativeKeyAddress(Data key) {
        ensureMemory();

        final int mask = allocatedSlotCount - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (accessor.isAssigned(slot)) {
            long slotKey = accessor.getKey(slot);
            if (NativeMemoryDataUtil.equals(slotKey, key)) {
                return slotKey;
            }
            slot = (slot + 1) & mask;
            if (slot == wrappedAround) {
                break;
            }
        }
        return NULL_ADDRESS;
    }

    private void ensureMemory() {
        if (accessor == null) {
            throw new IllegalStateException("Map is already destroyed!");
        }
    }

    @Override
    public boolean containsKey(Object k) {
        ensureMemory();

        Data key = (Data) k;
        final int mask = allocatedSlotCount - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (accessor.isAssigned(slot)) {
            long slotKey = accessor.getKey(slot);
            if (NativeMemoryDataUtil.equals(slotKey, key)) {
                return true;
            }
            slot = (slot + 1) & mask;
            if (slot == wrappedAround) {
                break;
            }
        }
        return false;
    }

    @Override
    public boolean containsValue(final Object v) {
        ensureMemory();

        V value = (V) v;
        for (int slot = 0; slot < allocatedSlotCount; slot++) {
            if (accessor.isAssigned(slot)) {
                long current = accessor.getValue(slot);
                if (memoryBlockProcessor.isEqual(current, value)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns a key iterator indented to be used for forced evictions - iteration order is (pseudo-)random.
     *
     * It expects a caller will remove each visited entry - failing to do means one entry can be visited
     * more than once and some other entry won't be visited at all.
     */
    public SlottableIterator<Data> newRandomEvictionKeyIterator() {
        return new RandomKeyIter();
    }

    /**
     * Returns a key iterator indented to be used for forced evictions - iteration order is (pseudo-)random.
     *
     * It expects a caller will remove each visited entry - failing to do means one entry can be visited
     * multiple-times and some other entry won't be visited at all.
     */
    public SlottableIterator<V> newRandomEvictionValueIterator() {
        return new RandomValueIter();
    }

    public SlottableIterator<Map.Entry<Data, V>> newRandomEvictionCachedEntryIterator() {
        return new CachedRandomEntryIter();
    }

    protected class RandomKeyIter extends RandomSlotIter<Data> {
        @Override
        public Data next() {
            nextSlot();
            return accessor.keyData(currentSlot);
        }
    }

    protected class RandomValueIter extends RandomSlotIter<V> {
        @Override
        public V next() {
            nextSlot();
            long slotValue = accessor.getValue(currentSlot);
            V value = readV(slotValue);
            return value;
        }
    }

    protected class CachedRandomEntryIter extends RandomSlotIter<Map.Entry<Data, V>> {
        MapEntry cachedEntry = new MapEntry();

        @Override
        public Map.Entry<Data, V> next() {
            nextSlot();
            return cachedEntry.init(currentSlot);
        }
    }

    private abstract class RandomSlotIter<E> implements SlottableIterator<E> {
        int currentSlot = -1;

        private int iterationCount;
        private int initialSize = assignedSlotCount;
        private int nextSlot = -1;
        private NativeMemoryData keyHolder = new NativeMemoryData();
        private int lastKnownModCount;

        RandomSlotIter() {
            lastKnownModCount = modCount;
            nextSlot = advanceAndIncrementIterations();
        }

        final int advanceAndIncrementIterations() {
            ensureMemory();
            if (iterationCount == initialSize) {
                return -1;
            }

            int slot = advance();
            iterationCount++;
            return slot;
        }

        final int advance() {
            ensureMemory();
            failIfModified(true, lastKnownModCount);

            int slot;
            do {
                slot = getRandom().nextInt(capacity());
            } while (!accessor.isAssigned(slot) || (slot == currentSlot));
            return slot;
        }

        @Override
        public final boolean hasNext() {
            return nextSlot > -1;
        }

        @Override
        public final int nextSlot() {
            ensureMemory();
            if (nextSlot < 0) {
                throw new NoSuchElementException();
            }

            failIfModified(true, lastKnownModCount);

            currentSlot = nextSlot;
            if (!accessor.isAssigned(currentSlot)) {
                currentSlot = advance();
                if (currentSlot < 0) {
                    throw new ConcurrentModificationException("Map was modified externally.");
                }
            }

            nextSlot = advanceAndIncrementIterations();
            return currentSlot;
        }

        @Override
        public final void remove() {
            removeInternal(true);
        }

        protected void removeInternal(boolean disposeKey) {
            ensureMemory();
            if (currentSlot < 0) {
                throw new NoSuchElementException();
            }

            failIfModified(true, lastKnownModCount);

            long key = accessor.getKey(currentSlot);
            long value = accessor.getValue(currentSlot);

            assignedSlotCount--;
            shiftConflictingKeys(currentSlot);

            if (disposeKey) {
                keyHolder.reset(key);
                memoryBlockProcessor.disposeData(keyHolder);
            }

            if (value != NULL_ADDRESS) {
                memoryBlockProcessor.dispose(value);
            }

            // if current slot is assigned after
            // removal and shift
            // then it means entry in the next slot
            // is moved to current slot
            if (accessor.isAssigned(currentSlot)) {
                nextSlot = currentSlot;
            }

            lastKnownModCount = modCount;
        }

        @Override
        public int getNextSlot() {
            return nextSlot;
        }

        @Override
        public int getCurrentSlot() {
            return currentSlot;
        }

    }

    private void failIfModified(boolean enable, int lastKnownModCount) {
        if (enable && lastKnownModCount != modCount) {
            throw new ConcurrentModificationException();
        }
    }

    private abstract class SlotIter<E> implements SlottableIterator<E> {
        int nextSlot = -1;
        int currentSlot = -1;
        private NativeMemoryData keyHolder;
        private boolean failFast;
        private int lastKnownModCount;

        SlotIter() {
            this(0, true);
        }

        SlotIter(boolean failFast) {
            this(0, failFast);
        }

        SlotIter(int startSlot, boolean failFast) {
            this.failFast = failFast;
            this.lastKnownModCount = modCount;
            this.nextSlot = advance(startSlot);
        }

        final int advance(int start) {
            ensureMemory();
            failIfModified(failFast, lastKnownModCount);

            for (int slot = start; slot < allocatedSlotCount; slot++) {
                if (accessor.isAssigned(slot)) {
                    return slot;
                }
            }
            return -1;
        }

        @Override
        public final boolean hasNext() {
            return nextSlot > -1;
        }

        @Override
        public final int nextSlot() {
            if (nextSlot < 0) {
                throw new NoSuchElementException();
            }
            failIfModified(failFast, lastKnownModCount);

            currentSlot = nextSlot;
            nextSlot = advance(nextSlot + 1);
            return currentSlot;
        }

        @Override
        public final void remove() {
            removeInternal(true);
        }

        protected void removeInternal(boolean disposeKey) {
            ensureMemory();
            if (currentSlot < 0) {
                throw new NoSuchElementException();
            }
            failIfModified(failFast, lastKnownModCount);

            long key = accessor.getKey(currentSlot);
            long value = accessor.getValue(currentSlot);

            assignedSlotCount--;
            shiftConflictingKeys(currentSlot);

            if (disposeKey) {
                memoryBlockProcessor.disposeData(readIntoKeyHolder(key));
            }

            if (value != NULL_ADDRESS) {
                memoryBlockProcessor.dispose(value);
            }

            // if current slot is assigned after
            // removal and shift
            // then it means entry in the next slot
            // is moved to current slot
            if (accessor.isAssigned(currentSlot)) {
                nextSlot = currentSlot;
            }

            lastKnownModCount = modCount;
        }

        private NativeMemoryData readIntoKeyHolder(long key) {
            if (keyHolder == null) {
                keyHolder = new NativeMemoryData();
            }
            keyHolder.reset(key);
            return keyHolder;
        }

        @Override
        public int getNextSlot() {
            return nextSlot;
        }

        @Override
        public int getCurrentSlot() {
            return currentSlot;
        }
    }

    @Override
    public Set<Data> keySet() {
        ensureMemory();
        return new KeySet();
    }

    private class KeySet extends AbstractSet<Data> {
        @Override
        public Iterator<Data> iterator() {
            return new KeyIter();
        }

        @Override
        public int size() {
            return BinaryElasticHashMap.this.size();
        }

        @Override
        public boolean contains(Object o) {
            return containsKey(o);
        }

        @Override
        public boolean remove(Object o) {
            return BinaryElasticHashMap.this.remove(o) != null;
        }

        @Override
        public void clear() {
            BinaryElasticHashMap.this.clear();
        }
    }

    protected KeyIter keyIter(int startSlot) {
        return new KeyIter(startSlot);
    }

    /**
     * Key iterator.
     */
    protected class KeyIter extends SlotIter<Data> implements Iterator<Data> {
        KeyIter() {
        }

        KeyIter(int startSlot) {
            super(startSlot, true);
        }

        @Override
        public Data next() {
            nextSlot();
            return accessor.keyData(currentSlot);
        }

        @Override
        public void removeInternal(boolean disposeKey) {
            super.removeInternal(disposeKey);
        }
    }

    @Override
    public Collection<V> values() {
        ensureMemory();
        return new Values();
    }

    private final class Values extends AbstractCollection<V> {
        @Override
        public Iterator<V> iterator() {
            return new ValueIter();
        }

        @Override
        public int size() {
            return BinaryElasticHashMap.this.size();
        }

        @Override
        public boolean contains(Object o) {
            return containsValue(o);
        }

        @Override
        public void clear() {
            BinaryElasticHashMap.this.clear();
        }
    }

    public final Iterator<V> valueIter() {
        return new ValueIter();
    }

    /**
     * @param failFast {@code true} to enable fail fast behaviour, otherwise set {@code false}
     * @return a new iterator instance
     * @see #modCount
     */
    public final Iterator<V> valueIter(boolean failFast) {
        return new ValueIter(failFast);
    }

    /**
     * Iterator over the map's values.
     */
    protected class ValueIter extends SlotIter<V> implements Iterator<V> {

        public ValueIter() {
        }

        public ValueIter(boolean failFast) {
            super(failFast);
        }

        @Override
        public V next() {
            nextSlot();
            long slotValue = accessor.getValue(currentSlot);
            return readV(slotValue);
        }
    }

    protected V readV(long slotValue) {
        if (slotValue == NULL_ADDRESS) {
            return null;
        }
        return memoryBlockProcessor.read(slotValue);
    }

    @Override
    public Set<Map.Entry<Data, V>> entrySet() {
        ensureMemory();
        return new EntrySet();
    }

    private final class EntrySet extends AbstractSet<Map.Entry<Data, V>> {
        @Override
        public Iterator<Map.Entry<Data, V>> iterator() {
            return new EntryIter();
        }

        @Override
        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }
            Map.Entry<Data, MemoryBlock> e = (Map.Entry<Data, MemoryBlock>) o;
            MemoryBlock value = get(e.getKey());
            if (value != null) {
                if (value.equals(e.getValue())) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean remove(Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }
            Map.Entry<Data, MemoryBlock> e = (Map.Entry<Data, MemoryBlock>) o;
            Data key = e.getKey();
            boolean deleted = delete(key);
//            if (deleted) {
//                serializationService.disposeData(key);
//                enqueueBinary((OffHeapBinary) key);
//            }
            return deleted;
        }

        @Override
        public int size() {
            return BinaryElasticHashMap.this.size();
        }

        @Override
        public void clear() {
            BinaryElasticHashMap.this.clear();
        }
    }

    /**
     * @param failFast {@code true} to enable fail fast behaviour, otherwise set {@code false}
     * @return a new iterator instance
     * @see #modCount
     */
    public final Iterator<Map.Entry<Data, V>> entryIter(boolean failFast) {
        return new EntryIter(failFast);
    }

    protected SlottableIterator<Map.Entry<Data, V>> entryIter(int slot) {
        return new EntryIter(slot);
    }

    private class EntryIter extends SlotIter<Map.Entry<Data, V>> {
        EntryIter() {
        }

        EntryIter(boolean failFast) {
            super(failFast);
        }

        EntryIter(int slot) {
            this(slot, true);
        }

        EntryIter(int slot, boolean failFast) {
            super(failFast);

            if (slot < 0 || slot > allocatedSlotCount) {
                slot = 0;
                //throw new IllegalArgumentException("Slot: " + slot + ", capacity: " + allocated);
            }
            nextSlot = advance(slot);
        }

        @Override
        public Map.Entry<Data, V> next() {
            nextSlot();
            return new MapEntry(currentSlot);
        }
    }

    public final Iterator<Map.Entry<Data, V>> cachedEntryIter(boolean failFast) {
        return new CachedEntryIter(failFast);
    }

    private class CachedEntryIter extends EntryIter {
        private MapEntry entry = new MapEntry();

        public CachedEntryIter(boolean failFast) {
            super(failFast);
        }

        @Override
        public Map.Entry<Data, V> next() {
            nextSlot();
            return entry.init(currentSlot);
        }
    }

    /**
     * {@code Map.Entry} implementation for this map.
     */
    protected class MapEntry implements Map.Entry {

        private int slot;

        protected MapEntry() {
        }

        protected MapEntry(int slot) {
            init(slot);
        }

        protected MapEntry init(int slot) {
            this.slot = slot;
            return this;
        }

        @Override
        public Object getKey() {
            return accessor.keyData(slot);
        }

        @Override
        public Object getValue() {
            final long value = accessor.getValue(slot);
            return readV(value);
        }

        @Override
        public Object setValue(Object value) {
            Object current = getValue();
            accessor.setValue(slot, (MemoryBlock) value);
            return current;
        }
    }

    /**
     * Clears the map by removing and disposing all key/value pairs stored.
     */
    @Override
    public void clear() {
        ensureMemory();
        modCount++;
        if (accessor != null) {
            KeyIter iter = new KeyIter();
            while (iter.hasNext()) {
                iter.nextSlot();
                iter.remove();
            }
            assignedSlotCount = 0;
            accessor.clear();
        }
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public NativeMemoryData storeHeaderOffHeap(MemoryAllocator malloc, long addressGiven) {
        long address = addressGiven;
        int size = HEADER_LENGTH_IN_BYTES + NativeMemoryData.NATIVE_MEMORY_DATA_OVERHEAD;

        if (addressGiven <= 0) {
            address = malloc.allocate(size);
        }
        long pointer = address;

        GlobalMemoryAccessor unsafe = GlobalMemoryAccessorRegistry.MEM;
        unsafe.putInt(pointer, HEADER_LENGTH_IN_BYTES);
        pointer += 4;
        unsafe.putInt(pointer, allocatedSlotCount);
        pointer += 4;
        unsafe.putInt(pointer, assignedSlotCount);
        pointer += 4;
        unsafe.putFloat(pointer, loadFactor);
        pointer += 4;
        unsafe.putInt(pointer, resizeAt);
        pointer += 4;
        unsafe.putInt(pointer, perturbation);
        pointer += 4;
        unsafe.putLong(pointer, accessor.baseAddr);
        pointer += 8;
        unsafe.putLong(pointer, accessor.size);

        return new NativeMemoryData(address, size);
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public static <V extends MemoryBlock> BinaryElasticHashMap<V> loadFromOffHeapHeader(EnterpriseSerializationService ss,
                                                                                        MemoryAllocator malloc, long address,
                                                                                        BehmSlotAccessorFactory behmSlotAccessorFactory,
                                                                                        MemoryBlockAccessor memoryBlockAccessor) {
        GlobalMemoryAccessor unsafe = GlobalMemoryAccessorRegistry.MEM;

        long pointer = address + NATIVE_MEMORY_DATA_OVERHEAD;
        int allocatedSlotCount = unsafe.getInt(pointer);
        pointer += 4;
        int assignedSlotCount = unsafe.getInt(pointer);
        pointer += 4;
        float loadFactor = unsafe.getFloat(pointer);
        pointer += 4;
        int resizeAt = unsafe.getInt(pointer);
        pointer += 4;
        int perturbation = unsafe.getInt(pointer);
        pointer += 4;
        long baseAddr = unsafe.getLong(pointer);
        pointer += 8;
        long size = unsafe.getLong(pointer);

        return new BinaryElasticHashMap<V>(allocatedSlotCount, assignedSlotCount, loadFactor, resizeAt, perturbation,
                baseAddr, size, malloc, ss, behmSlotAccessorFactory, memoryBlockAccessor);
    }

    /**
     * Disposes internal backing array of this map. Does not dispose key/value pairs inside.
     * To dispose key/value pairs, {@link #clear()} must be called explicitly.
     *
     * @see #clear()
     */
    @Override
    public void dispose() {
        if (accessor != null) {
            accessor.delete();
        }
        allocatedSlotCount = 0;
        accessor = null;
        resizeAt = 0;
    }

    @Override
    public int size() {
        return assignedSlotCount;
    }

    public int capacity() {
        return allocatedSlotCount;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public String toString() {
        return "BinaryElasticHashMap{address=" + accessor
                + ", allocated=" + allocatedSlotCount
                + ", assigned=" + assignedSlotCount
                + ", loadFactor=" + loadFactor
                + ", resizeAt=" + resizeAt + '}';
    }

}
