package com.hazelcast.elastic.map;

import com.hazelcast.elastic.SlottableIterator;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.memory.MemoryBlockAccessor;
import com.hazelcast.memory.MemoryBlockProcessor;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.DefaultData;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.NativeMemoryData;
import com.hazelcast.nio.serialization.NativeMemoryDataUtil;
import com.hazelcast.util.HashUtil;
import com.hazelcast.util.QuickMath;
import sun.misc.Unsafe;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * A hash map of <code>Data</code> to <code>MemoryBlock</code>, implemented using open
 * addressing with linear probing for collision resolution.
 * <p>
 * The internal buffers of this implementation ({@link #keys}, {@link #values},
 * {@link #allocated}) are always allocated to the nearest size that is a power of two. When
 * the capacity exceeds the given load factor, the buffer size is doubled.
 * </p>
 *
 * @author This code is inspired by the collaboration and implementation in the
 *         <a href="http://fastutil.dsi.unimi.it/">fastutil</a> project.
 */
// TODO Can we move key + value to the same cache line?
public class BinaryElasticHashMap<V extends MemoryBlock> implements ElasticMap<Data, V> {

    private static final long allocationFactor = 17L;

    protected final MemoryBlockProcessor<V> memoryBlockProcessor;

    private long baseAddress;

    /**
     * Hash-indexed array holding all keys.
     *
     * @see #values
     */
    private long keys; // long[] keys

    /**
     * Hash-indexed array holding all values associated to the keys
     * stored in {@link #keys}.
     *
     * @see #keys
     */
    private long values; // long[] values

    /**
     * Information if an entry (slot) in the {@link #values} table is allocated
     * or empty.
     *
     * @see #assigned
     */
    private long allocated;  // boolean[] allocated

    private int allocatedLength;

    /**
     * Cached number of assigned slots in {@link #allocated}.
     */
    private int assigned;

    /**
     * The load factor for this map (fraction of allocated slots
     * before the buffers must be rehashed or reallocated).
     */
    private final float loadFactor;

    /**
     * Resize buffers when {@link #allocatedLength} hits this value.
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

    /**
     * Creates a hash map with the default capacity of {@value #DEFAULT_CAPACITY},
     * load factor of {@value #DEFAULT_LOAD_FACTOR}.
     */
    public BinaryElasticHashMap(EnterpriseSerializationService serializationService,
            MemoryBlockAccessor<V> memoryBlockAccessor, MemoryAllocator malloc) {
        this(DEFAULT_CAPACITY, serializationService, memoryBlockAccessor, malloc);
    }

    /**
     * Creates a hash map with the given initial capacity, default load factor of
     * {@value #DEFAULT_LOAD_FACTOR}.
     *
     * @param initialCapacity Initial capacity (greater than zero and automatically
     *                        rounded to the next power of two).
     * @param serializationService
     * @param memoryBlockAccessor
     * @param malloc
     */
    public BinaryElasticHashMap(int initialCapacity, EnterpriseSerializationService serializationService,
            MemoryBlockAccessor<V> memoryBlockAccessor, MemoryAllocator malloc) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR, serializationService, memoryBlockAccessor, malloc);
    }

    /**
     * Creates a hash map with the given initial capacity,
     * load factor.
     *  @param initialCapacity Initial capacity (greater than zero and automatically
     *                        rounded to the next power of two).
     * @param loadFactor      The load factor (greater than zero and smaller than 1).
     * @param serializationService
     * @param memoryBlockAccessor
     * @param malloc
     */
    public BinaryElasticHashMap(int initialCapacity, float loadFactor,
            EnterpriseSerializationService serializationService,
            MemoryBlockAccessor<V> memoryBlockAccessor, MemoryAllocator malloc) {
        this(initialCapacity, loadFactor,
                new BinaryElasticHashMapMemoryBlockProcessor<V>(serializationService,
                        memoryBlockAccessor, malloc));
    }

    /**
     * Creates a hash map with the given initial capacity,
     * load factor.
     *  @param initialCapacity Initial capacity (greater than zero and automatically
     *                        rounded to the next power of two).
     * @param memoryBlockProcessor
     */
    public BinaryElasticHashMap(int initialCapacity, MemoryBlockProcessor<V> memoryBlockProcessor) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR, memoryBlockProcessor);
    }

    /**
     * Creates a hash map with the given initial capacity,
     * load factor.
     *  @param initialCapacity Initial capacity (greater than zero and automatically
     *                        rounded to the next power of two).
     * @param loadFactor      The load factor (greater than zero and smaller than 1).
     * @param memoryBlockProcessor
     */
    public BinaryElasticHashMap(int initialCapacity, float loadFactor,
            MemoryBlockProcessor<V> memoryBlockProcessor) {
        initialCapacity = Math.max(initialCapacity, MIN_CAPACITY);

        assert initialCapacity > 0
                : "Initial capacity must be between (0, " + Integer.MAX_VALUE + "].";
        assert loadFactor > 0 && loadFactor <= 1
                : "Load factor must be between (0, 1].";

        this.loadFactor = loadFactor;
        this.memoryBlockProcessor = memoryBlockProcessor;
        this.malloc = memoryBlockProcessor.unwrapMemoryAllocator();

        initialCapacity = roundCapacity(initialCapacity);
        allocateBuffers(initialCapacity);
    }

    /**
     * Allocate internal buffers for a given capacity.
     *
     * @param capacity New capacity (must be a power of two).
     */
    private void allocateBuffers(int capacity) {
        long allocationCapacity = capacity * allocationFactor;
        try {
            baseAddress = malloc.allocate(allocationCapacity);
        } catch (NativeOutOfMemoryError e) {
            throw onOome(e);
        }
        UnsafeHelper.UNSAFE.setMemory(baseAddress, allocationCapacity, (byte) 0);

        // TODO: can we move key + value to the same cache line?
        keys = baseAddress;
        values = keys + (capacity * 8L);
        allocated = values + (capacity * 8L);

        allocatedLength = capacity;
        resizeAt = Math.max(2, (int) Math.ceil(capacity * loadFactor)) - 1;
        perturbation = computePerturbationValue(capacity);
    }

    protected NativeOutOfMemoryError onOome(NativeOutOfMemoryError e) {
        return e;
    }

    /**
     * Round the capacity to the next allowed value.
     */
    public static int roundCapacity(int requestedCapacity) {
        if (requestedCapacity > MAX_CAPACITY)
            return MAX_CAPACITY;

        return Math.max(MIN_CAPACITY, QuickMath.nextPowerOfTwo(requestedCapacity));
    }

    /**
     * Return the next possible capacity, counting from the current buffers'
     * size.
     */
    public static int nextCapacity(int current) {
        assert current > 0 && Long.bitCount(current) == 1 : "Capacity must be a power of two.";

        if (current < MIN_CAPACITY / 2) {
            current = MIN_CAPACITY / 2;
        }

        current <<= 1;
        if (current < 0) {
            throw new RuntimeException("Maximum capacity exceeded.");
        }
        return current;
    }

    @Override
    public V put(Data key, V value) {
        ensureMemory();
        assert assigned < allocatedLength;

        final int mask = allocatedLength - 1;
        int slot = rehash(key, perturbation) & mask;
        while (isAllocated(slot)) {
            long slotKey = getKey(slot);
            if (NativeMemoryDataUtil.equals(slotKey, key)) {
                final long oldValue = getValue(slot);
                setValue(slot, value.address());
                if (key instanceof NativeMemoryData && ((NativeMemoryData) key).address() != slotKey) {
                    memoryBlockProcessor.disposeData(key);
                }
                return memoryBlockProcessor.read(oldValue);
            }
            slot = (slot + 1) & mask;
        }

        NativeMemoryData memKey = (NativeMemoryData) memoryBlockProcessor.convertData(key, DataType.NATIVE);
        // Check if we need to grow. If so, reallocate new data, fill in the last element
        // and rehash.
        if (assigned == resizeAt) {
            expandAndPut(memKey.address(), value.address(), slot);
        } else {
            assigned++;
            setAllocated(slot, true);
            setKey(slot, memKey.address());
            setValue(slot, value.address());
        }
        return null;
    }

    @Override
    public boolean set(Data key, V value) {
        V old = put(key, value);
        if (old != null) {
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
        assert newValue instanceof NativeMemoryData;

        final int mask = allocatedLength - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (isAllocated(slot)) {
            long slotKey = getKey(slot);
            if (NativeMemoryDataUtil.equals(slotKey, key)) {
                long current = getValue(slot);
                if (memoryBlockProcessor.isEqual(current, oldValue)) {
                    setValue(slot, newValue.address());
                    memoryBlockProcessor.dispose(current);
                    return true;
                }
                return false;
            }
            slot = (slot + 1) & mask;
            if (slot == wrappedAround) break;
        }
        return false;
    }

    @Override
    public V replace(Data key, MemoryBlock value) {
        ensureMemory();
        final int mask = allocatedLength - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (isAllocated(slot)) {
            long slotKey = getKey(slot);
            if (NativeMemoryDataUtil.equals(slotKey, key)) {
                long current = getValue(slot);
                setValue(slot, value.address());
                return memoryBlockProcessor.read(current);
            }
            slot = (slot + 1) & mask;
            if (slot == wrappedAround) break;
        }
        return null;
    }

    private NativeMemoryData readData(long address) {
        if (address > 0L) {
            return new NativeMemoryData().reset(address);
        }
        return null;
    }

    /**
     * Expand the internal storage buffers (capacity) and rehash.
     */
    private void expandAndPut(long pendingKey, long pendingValue, int freeSlot) {
        ensureMemory();
        assert assigned == resizeAt;
        assert !isAllocated(freeSlot);

        // Try to allocate new buffers first. If we OOM, it'll be now without
        // leaving the data structure in an inconsistent state.
        final long oldAddress = baseAddress;
        final long oldKeys = keys;
        final long oldValues = values;
        final long oldAllocated = allocated;
        final int oldAllocatedLength = allocatedLength;

        allocateBuffers(nextCapacity(allocatedLength));

        // We have succeeded at allocating new data so insert the pending key/value at
        // the free slot in the temp arrays before rehashing.
        assigned++;
        writeBool(oldAllocated, freeSlot, true);
        writeLong(oldKeys, freeSlot, pendingKey);
        writeLong(oldValues, freeSlot, pendingValue);

        // Rehash all stored keys into the new buffers.
        final int mask = allocatedLength - 1;
        for (int i = oldAllocatedLength; --i >= 0; ) {
            if (readBool(oldAllocated, i)) {
                final long k = readLong(oldKeys, i);
                final long v = readLong(oldValues, i);

                int slot = rehash(NativeMemoryDataUtil.hashCode(k), perturbation) & mask;
                while (isAllocated(slot)) {
                    slot = (slot + 1) & mask;
                }

                setAllocated(slot, true);
                setKey(slot, k);
                setValue(slot, v);
            }
        }
        malloc.free(oldAddress, oldAllocatedLength * allocationFactor);
    }

    @Override
    public V remove(Object k) {
        ensureMemory();
        Data key = (Data) k;
        final int mask = allocatedLength - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (isAllocated(slot)) {
            long slotKey = getKey(slot);
            if (NativeMemoryDataUtil.equals(slotKey, key)) {
                assigned--;
                long v = getValue(slot);
                if (key instanceof DefaultData ||
                        (key instanceof NativeMemoryData && ((NativeMemoryData) key).address() != slotKey)) {
                    memoryBlockProcessor.disposeData(readData(slotKey));
                }
                shiftConflictingKeys(slot);
                return memoryBlockProcessor.read(v);
            }
            slot = (slot + 1) & mask;
            if (slot == wrappedAround) break;
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

        final int mask = allocatedLength - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (isAllocated(slot)) {
            long slotKey = getKey(slot);
            if (NativeMemoryDataUtil.equals(slotKey, key)) {
                long current = getValue(slot);
                if (memoryBlockProcessor.isEqual(current, value)){
                    assigned--;
                    if (key instanceof DefaultData ||
                            (key instanceof NativeMemoryData && ((NativeMemoryData) key).address() != slotKey)) {
                        memoryBlockProcessor.disposeData(readData(slotKey));
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
            if (slot == wrappedAround) break;
        }

        return false;
    }

    /**
     * Shift all the slot-conflicting keys allocated to (and including) <code>slot</code>.
     */
    protected void shiftConflictingKeys(int slotCurr) {
        // Copied nearly verbatim from fastutil's impl.
        final int mask = allocatedLength - 1;
        int slotPrev, slotOther;
        while (true) {
            slotCurr = ((slotPrev = slotCurr) + 1) & mask;

            while (isAllocated(slotCurr)) {
                slotOther = rehash(NativeMemoryDataUtil.hashCode(getKey(slotCurr)), perturbation) & mask;

                if (slotPrev <= slotCurr) {
                    // we're on the right of the original slot.
                    if (slotPrev >= slotOther || slotOther > slotCurr)
                        break;
                } else {
                    // we've wrapped around.
                    if (slotPrev >= slotOther && slotOther > slotCurr)
                        break;
                }
                slotCurr = (slotCurr + 1) & mask;
            }

            if (!isAllocated(slotCurr)) {
                break;
            }

            // Shift key/value pair.
            setKey(slotPrev, getKey(slotCurr));
            setValue(slotPrev, getValue(slotCurr));
        }

        setAllocated(slotPrev, false);
        setKey(slotPrev, 0L);
        setValue(slotPrev, 0L);
    }

    @Override
    public V get(Object k) {
        ensureMemory();

        Data key = (Data) k;
        final int mask = allocatedLength - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (isAllocated(slot)) {
            long slotKey = getKey(slot);
            if (NativeMemoryDataUtil.equals(slotKey, key)) {
                long value = getValue(slot);
                return memoryBlockProcessor.read(value);
            }
            slot = (slot + 1) & mask;
            if (slot == wrappedAround) break;
        }
        return null;
    }

    private void ensureMemory() {
        if (baseAddress < 0L) {
            throw new IllegalStateException("Map is already destroyed!");
        }
    }

    @Override
    public boolean containsKey(Object k) {
        ensureMemory();

        Data key = (Data) k;
        final int mask = allocatedLength - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (isAllocated(slot)) {
            long slotKey = getKey(slot);
            if (NativeMemoryDataUtil.equals(slotKey, key)) {
                return true;
            }
            slot = (slot + 1) & mask;
            if (slot == wrappedAround) break;
        }
        return false;
    }

    @Override
    public boolean containsValue(final Object v) {
        ensureMemory();

        V value = (V) v;
        for (int slot = 0; slot < allocatedLength; slot++) {
            if (isAllocated(slot)) {
                long current = getValue(slot);
                if (memoryBlockProcessor.isEqual(current, value)) {
                    return true;
                }
            }
        }
        return false;
    }

    private abstract class SlotIter<E> implements SlottableIterator<E> {
        int nextSlot = -1;
        int currentSlot = -1;

        SlotIter() {
            nextSlot = advance(0);
        }

        @Override
        public final int advance(int start) {
            ensureMemory();
            for (int slot = start; slot < allocatedLength; slot++) {
                if (isAllocated(slot)) {
                    return slot;
                }
            }
            return -1;
        }

        public final boolean hasNext() {
            return nextSlot > -1;
        }

        @Override
        public final int nextSlot() {
            if (nextSlot < 0) {
                throw new NoSuchElementException();
            }
            currentSlot = nextSlot;
            nextSlot = advance(nextSlot + 1);
            return currentSlot;
        }

        public final void remove() {
            ensureMemory();
            if (currentSlot < 0) {
                throw new NoSuchElementException();
            }
            assigned--;
            long current = getValue(currentSlot);
            memoryBlockProcessor.dispose(current);
            shiftConflictingKeys(currentSlot);
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
        return new KeySet();
    }

    private class KeySet extends AbstractSet<Data> {
        public Iterator<Data> iterator() {
            return new KeyIter();
        }
        public int size() {
            return BinaryElasticHashMap.this.size();
        }
        public boolean contains(Object o) {
            return containsKey(o);
        }
        public boolean remove(Object o) {
            return BinaryElasticHashMap.this.remove(o) != null;
        }
        public void clear() {
            BinaryElasticHashMap.this.clear();
        }
    }

    public class KeyIter extends SlotIter<Data> implements Iterator<Data> {
        @Override
        public Data next() {
            nextSlot();
            long slotKey = getKey(currentSlot);
            return readData(slotKey);
        }
    }

    @Override
    public Collection<V> values() {
        return new Values();
    }

    private final class Values extends AbstractCollection<V> {
        public Iterator<V> iterator() {
            return new ValueIter();
        }
        public int size() {
            return BinaryElasticHashMap.this.size();
        }
        public boolean contains(Object o) {
            return containsValue(o);
        }
        public void clear() {
            BinaryElasticHashMap.this.clear();
        }
    }

    public class ValueIter extends SlotIter<V> implements Iterator<V> {
        @Override
        public V next() {
            nextSlot();
            long slotValue = getValue(currentSlot);
            return memoryBlockProcessor.read(slotValue);
        }
    }

    @Override
    public Set<Map.Entry<Data, V>> entrySet() {
        return new EntrySet();
    }

    private final class EntrySet extends AbstractSet<Map.Entry<Data, V>> {
        public Iterator<Map.Entry<Data, V>> iterator() {
            return new EntryIter();
        }
        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<Data, MemoryBlock> e = (Map.Entry<Data, MemoryBlock>) o;
            MemoryBlock value = get(e.getKey());
            if (value != null) {
                if (value.equals(e.getValue())) {
                    return true;
                }
            }
            return false;
        }
        public boolean remove(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<Data, MemoryBlock> e = (Map.Entry<Data, MemoryBlock>) o;
            Data key = e.getKey();
            boolean deleted = delete(key);
//            if (deleted) {
//                serializationService.disposeData(key);
//                enqueueBinary((OffHeapBinary) key);
//            }
            return deleted;
        }
        public int size() {
            return BinaryElasticHashMap.this.size();
        }
        public void clear() {
            BinaryElasticHashMap.this.clear();
        }
    }

    public class EntryIter extends SlotIter<Map.Entry<Data, V>> {
        public EntryIter() {
        }

        public EntryIter(int slot) {
            if (slot < 0 || slot > allocatedLength) {
                slot = 0;
                //throw new IllegalArgumentException("Slot: " + slot + ", capacity: " + allocatedLength);
            }
            nextSlot = advance(slot);
        }

        @Override
        public Map.Entry<Data, V> next() {
            nextSlot();
            return new MapEntry(currentSlot);
        }
    }

    protected class MapEntry implements Map.Entry<Data, V> {

        private final int slot;

        protected MapEntry(final int slot) {
            this.slot = slot;
        }

        @Override
        public Data getKey() {
            return readData(BinaryElasticHashMap.this.getKey(slot));
        }

        @Override
        public V getValue() {
            return memoryBlockProcessor.read(BinaryElasticHashMap.this.getValue(slot));
        }

        @Override
        public V setValue(MemoryBlock value) {
            V current = getValue();
            BinaryElasticHashMap.this.setValue(slot, value.address());
            return current;
        }
    }

    /**
     * <p>Does not release internal buffers.</p>
     */
    @Override
    public void clear() {
        if (baseAddress > 0L) {
            EntryIter iter = new EntryIter();
            while (iter.hasNext()) {
                Entry<Data, V> e = iter.next();
                memoryBlockProcessor.disposeData(e.getKey());
                memoryBlockProcessor.dispose(e.getValue());
            }

            assigned = 0;
            Unsafe unsafe = UnsafeHelper.UNSAFE;
            unsafe.setMemory(keys, allocatedLength * 8L, (byte) 0);
            unsafe.setMemory(values, allocatedLength * 8L, (byte) 0);
            unsafe.setMemory(allocated, allocatedLength, (byte) 0);
        }
    }

    @Override
    public void destroy() {
        if (assigned > 0) {
            clear();
        }
        if (baseAddress > 0L) {
            malloc.free(baseAddress, allocatedLength * allocationFactor);
        }
        allocatedLength = 0;
        baseAddress = -1L;
        keys = -1L;
        values = -1L;
        allocated = -1L;
        resizeAt = 0;
    }

    @Override
    public int size() {
        return assigned;
    }

    public int capacity() {
        return allocatedLength;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    protected long getKey(int index) {
        return UnsafeHelper.UNSAFE.getLong(keys + (index * 8L));
    }

    private void setKey(int index, long key) {
        UnsafeHelper.UNSAFE.putLong(keys + (index * 8L), key);
    }

    protected long getValue(int index) {
        return UnsafeHelper.UNSAFE.getLong(values + (index * 8L));
    }

    private void setValue(int index, long value) {
        UnsafeHelper.UNSAFE.putLong(values + (index * 8L), value);
    }

    protected boolean isAllocated(int index) {
        return UnsafeHelper.UNSAFE.getByte(allocated + index) != 0;
    }

    private void setAllocated(int index, boolean b) {
        UnsafeHelper.UNSAFE.putByte(allocated + index, (byte) (b ? 1 : 0));
    }

    protected static long readLong(long address, int index) {
        return UnsafeHelper.UNSAFE.getLong(address + (index * 8L));
    }

    private static void writeLong(long address, int index, long value) {
        UnsafeHelper.UNSAFE.putLong(address + (index * 8L), value);
    }

    protected static boolean readBool(long address, int index) {
        return UnsafeHelper.UNSAFE.getByte(address + index) != 0;
    }

    private static void writeBool(long address, int index, boolean b) {
        UnsafeHelper.UNSAFE.putByte(address + index, (byte) (b ? 1 : 0));
    }

    private static int rehash(Data o, int p) {
        return o == null ? 0 : HashUtil.MurmurHash3_fmix(o.hashCode() ^ p);
    }

    private static int rehash(int v, int p) {
        return HashUtil.MurmurHash3_fmix(v ^ p);
    }

    /**
     * Computer static perturbations table.
     */
    private final static int[] PERTURBATIONS = new Callable<int[]>() {
        public int[] call() {
            int[] result = new int[32];
            for (int i = 0; i < result.length; i++) {
                result[i] = HashUtil.MurmurHash3_fmix(17 + i);
            }
            return result;
        }
    }.call();


    /**
     * <p>Compute the key perturbation value applied before hashing. The returned value
     * should be non-zero and ideally different for each capacity. This matters because
     * keys are nearly-ordered by their hashed values so when adding one container's
     * values to the other, the number of collisions can skyrocket into the worst case
     * possible.
     * <p/>
     * <p>If it is known that hash containers will not be added to each other
     * (will be used for counting only, for example) then some speed can be gained by
     * not perturbing keys before hashing and returning a value of zero for all possible
     * capacities. The speed gain is a result of faster rehash operation (keys are mostly
     * in order).
     */
    private static int computePerturbationValue(int capacity) {
        return PERTURBATIONS[Integer.numberOfLeadingZeros(capacity)];
    }

    private static class BinaryElasticHashMapMemoryBlockProcessor<V extends MemoryBlock>
            implements MemoryBlockProcessor<V> {

        private final EnterpriseSerializationService serializationService;
        private final MemoryBlockAccessor<V> memoryBlockAccessor;
        private final MemoryAllocator malloc;

        private BinaryElasticHashMapMemoryBlockProcessor(EnterpriseSerializationService serializationService,
                MemoryBlockAccessor<V> memoryBlockAccessor, MemoryAllocator malloc) {
            this.serializationService = serializationService;
            this.memoryBlockAccessor = memoryBlockAccessor;
            this.malloc = malloc;
        }

        @Override
        public boolean isEqual(long address, V value) {
            return memoryBlockAccessor.isEqual(address, value);
        }

        @Override
        public boolean isEqual(long address1, long address2) {
            return memoryBlockAccessor.isEqual(address1, address2);
        }

        @Override
        public V read(long address) {
            return memoryBlockAccessor.read(address);
        }

        @Override
        public long dispose(long address) {
            return memoryBlockAccessor.dispose(address);
        }

        @Override
        public long dispose(V block) {
            return memoryBlockAccessor.dispose(block);
        }

        @Override
        public Data toData(Object obj, DataType dataType) {
            return serializationService.toData(obj, dataType);
        }

        @Override
        public Data convertData(Data data, DataType dataType) {
            return serializationService.convertData(data, dataType);
        }

        @Override
        public void disposeData(Data data) {
            serializationService.disposeData(data);
        }

        @Override
        public long allocate(long size) {
            return malloc.allocate(size);
        }

        @Override
        public void free(long address, long size) {
            malloc.free(address, size);
        }

        @Override
        public MemoryAllocator unwrapMemoryAllocator() {
            return malloc;
        }
    }

}
