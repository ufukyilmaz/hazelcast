package com.hazelcast.elastic.map.long2long;

import com.hazelcast.elastic.CapacityUtil;
import com.hazelcast.elastic.SlottableIterator;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.util.ExceptionUtil;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.hazelcast.elastic.CapacityUtil.DEFAULT_LOAD_FACTOR;
import static com.hazelcast.elastic.CapacityUtil.MIN_CAPACITY;
import static com.hazelcast.elastic.CapacityUtil.nextCapacity;
import static com.hazelcast.elastic.CapacityUtil.roundCapacity;
import static com.hazelcast.elastic.map.long2long.Long2LongSlotAccessor.rehash;
import static com.hazelcast.util.HashUtil.computePerturbationValue;

/**
 * <p>
 * A hash map of long to long, implemented using open
 * addressing with linear probing for collision resolution.
 * </p>
 *
 * <p>
 * The internal buffer of this implementation is
 * always allocated to the nearest higher size that is a power of two. When
 * the capacity exceeds the given load factor, the buffer size is doubled.
 * </p>
 */
@SuppressWarnings("checkstyle:methodcount")
public class Long2LongElasticHashMap implements Long2LongElasticMap {

    private Long2LongSlotAccessor accessor;

    /** Number of allocated slots */
    private int allocatedSlotCount;

    /** Cached number of assigned slots in {@link #allocatedSlotCount}. */
    private int assignedSlotCount;

    /**
     * The load factor for this map (fraction of allocated slots
     * before the buffers must be rehashed or reallocated).
     */
    private final float loadFactor;

    /** Resize buffers when {@link #assignedSlotCount} hits this value. */
    private int resizeAt;

    /**
     * We perturb hashed values with the array size to avoid problems with
     * nearly-sorted-by-hash values on iterations.
     *
     * @see "http://issues.carrot2.org/browse/HPPC-80"
     */
    private int perturbation;

    private final MemoryAllocator malloc;

    private long missingValue;

    /**
     * Creates a hash map with the default capacity of {@value com.hazelcast.elastic.CapacityUtil#DEFAULT_CAPACITY},
     * load factor of {@value com.hazelcast.elastic.CapacityUtil#DEFAULT_LOAD_FACTOR}.
     */
    public Long2LongElasticHashMap(MemoryAllocator malloc, long missingValue) {
        // Checkstyle complains that CapacityUtil is an unused import so we use it once here.
        this(CapacityUtil.DEFAULT_CAPACITY, malloc, missingValue);
    }

    /**
     * Creates a hash map with the given initial capacity, default load factor of
     * {@value com.hazelcast.elastic.CapacityUtil#DEFAULT_LOAD_FACTOR}.
     *
     * @param initialCapacity Initial capacity (greater than zero and automatically
     *                        rounded to the next power of two).
     */
    public Long2LongElasticHashMap(int initialCapacity, MemoryAllocator malloc,
                                   long missingValue) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR, malloc, missingValue);
    }

    /**
     * Creates a hash map with the given initial capacity,
     * load factor.
     *  @param initialCapacity Initial capacity (greater than zero and automatically
     *                        rounded to the next power of two).
     * @param loadFactor      The load factor (greater than zero and smaller than 1).
     */
    public Long2LongElasticHashMap(int initialCapacity, float loadFactor,
                                   MemoryAllocator malloc, long missingValue) {
        initialCapacity = Math.max(initialCapacity, MIN_CAPACITY);

        assert initialCapacity > 0
                : "Initial capacity must be between (0, " + Integer.MAX_VALUE + "].";
        assert loadFactor > 0 && loadFactor <= 1
                : "Load factor must be between (0, 1].";

        this.loadFactor = loadFactor;
        this.malloc = malloc;
        this.missingValue = missingValue;

        allocateBuffer(roundCapacity(initialCapacity));
    }

    /**
     * Allocate internal buffer for a given capacity.
     *
     * @param capacity New capacity (must be a power of two).
     */
    private void allocateBuffer(int capacity) {
        try {
            accessor = new Long2LongSlotAccessor(malloc, missingValue).allocate(capacity);
        } catch (NativeOutOfMemoryError e) {
            throw onOome(e);
        }
        allocatedSlotCount = capacity;
        resizeAt = Math.max(2, (int) Math.ceil(capacity * loadFactor)) - 1;
        perturbation = computePerturbationValue(capacity);
    }

    private NativeOutOfMemoryError onOome(NativeOutOfMemoryError e) {
        return e;
    }

    private void ensureMemory() {
        if (accessor == null) {
            throw new IllegalStateException("Map is already destroyed!");
        }
    }

    private void checkKey(long key) {
        if (missingValue == key) {
            throw new IllegalArgumentException("Invalid key: " + key);
        }
    }

    private void checkValue(long value) {
        if (missingValue == value) {
            throw new IllegalArgumentException("Invalid value: " + value);
        }
    }

    private void checkKeyAndValue(long key, long value) {
        if (missingValue == key) {
            throw new IllegalArgumentException("Invalid key: " + key);
        }
        if (missingValue == value) {
            throw new IllegalArgumentException("Invalid value: " + value);
        }
    }

    private void checkKeyAndValues(long key, long oldValue, long newValue) {
        if (missingValue == key) {
            throw new IllegalArgumentException("Invalid key: " + key);
        }
        if (missingValue == oldValue) {
            throw new IllegalArgumentException("Invalid old value: " + oldValue);
        }
        if (missingValue == newValue) {
            throw new IllegalArgumentException("Invalid new value: " + newValue);
        }
    }

    @Override
    public Long get(Object key) {
        return get((long) (Long) key);
    }

    @Override
    public long get(long key) {
        checkKey(key);

        ensureMemory();

        final int mask = allocatedSlotCount - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (accessor.isAssigned(slot)) {
            if (accessor.getKey(slot) == key) {
                return accessor.getValue(slot);
            }
            slot = (slot + 1) & mask;
            if (slot == wrappedAround) {
                break;
            }
        }
        return missingValue;
    }

    @Override
    public Long put(Long key, Long value) {
        return put(key.longValue(), value.longValue());
    }

    @Override
    public long put(long key, long value) {
        checkKeyAndValue(key, value);

        ensureMemory();
        assert assignedSlotCount < allocatedSlotCount;

        final int mask = allocatedSlotCount - 1;
        int slot = rehash(key, perturbation) & mask;
        while (accessor.isAssigned(slot)) {
            if (key == accessor.getKey(slot)) {
                return accessor.getValue(slot);
            }
            slot = (slot + 1) & mask;
        }

        // Check if we need to grow.
        // If so, reallocate new data, fill in the last element and rehash.
        if (assignedSlotCount == resizeAt) {
            try {
                expandAndPut(key, value, slot);
            } catch (Throwable error) {
                throw ExceptionUtil.rethrow(error);
            }

        } else {
            assignedSlotCount++;
            accessor.setKey(slot, key);
            accessor.setValue(slot, value);
        }
        return missingValue;
    }

    @Override
    public boolean set(Long key, Long value) {
        return set(key.longValue(), value.longValue());
    }

    @Override
    public boolean set(long key, long value) {
        return put(key, value) == missingValue;
    }

    @Override
    public Long putIfAbsent(Long key, Long value) {
        return putIfAbsent(key.longValue(), value.longValue());
    }

    @Override
    public long putIfAbsent(long key, long value) {
        checkKeyAndValue(key, value);

        long current = get(key);
        if (current == missingValue) {
            set(key, value);
            return missingValue;
        }
        return current;
    }

    @Override
    public void putAll(Map<? extends Long, ? extends Long> map) {
        for (Map.Entry<? extends Long, ? extends Long> entry : map.entrySet()) {
            set(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public Long replace(Long key, Long value) {
        return replace(key.longValue(), value.longValue());
    }

    @Override
    public long replace(long key, long value) {
        checkKeyAndValue(key, value);

        ensureMemory();

        final int mask = allocatedSlotCount - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (accessor.isAssigned(slot)) {
            if (accessor.getKey(slot) == key) {
                long current = accessor.getValue(slot);
                accessor.setValue(slot, value);
                return current;
            }
            slot = (slot + 1) & mask;
            if (slot == wrappedAround) {
                break;
            }
        }
        return missingValue;
    }

    @Override
    public boolean replace(Long key, Long oldValue, Long newValue) {
        return replace(key.longValue(), oldValue.longValue(), newValue.longValue());
    }

    @Override
    public boolean replace(long key, long oldValue, long newValue) {
        checkKeyAndValues(key, oldValue, newValue);

        ensureMemory();

        final int mask = allocatedSlotCount - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (accessor.isAssigned(slot)) {
            if (accessor.getKey(slot) == key) {
                if (accessor.getValue(slot) == oldValue) {
                    accessor.setValue(slot, newValue);
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
     * Expand the internal storage buffers (capacity) and rehash.
     */
    private void expandAndPut(long pendingKey, long pendingValue, int freeSlot) {
        ensureMemory();

        assert assignedSlotCount == resizeAt;
        assert !accessor.isAssigned(freeSlot);

        final Long2LongSlotAccessor oldAccessor = new Long2LongSlotAccessor(accessor);
        final int oldAllocated = allocatedSlotCount;

        // Try to allocate new buffer first. If we OOM, it'll be now without
        // leaving the data structure in an inconsistent state.
        allocateBuffer(nextCapacity(allocatedSlotCount));

        // We have succeeded at allocating new data so insert the pending key/value at
        // the free slot in the temp arrays before rehashing.
        assignedSlotCount++;
        oldAccessor.setKey(freeSlot, pendingKey);
        oldAccessor.setValue(freeSlot, pendingValue);

        // Rehash all stored keys into the new buffers.
        final int mask = allocatedSlotCount - 1;
        for (int slot = oldAllocated; --slot >= 0;) {
            if (oldAccessor.isAssigned(slot)) {
                long key = oldAccessor.getKey(slot);
                long value = oldAccessor.getValue(slot);
                int newSlot = rehash(key, perturbation) & mask;
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
    public Long remove(Object key) {
        return remove((long) (Long) key);
    }

    @Override
    public long remove(long key) {
        checkKey(key);

        ensureMemory();

        final int mask = allocatedSlotCount - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (accessor.isAssigned(slot)) {
            if (accessor.getKey(slot) == key) {
                assignedSlotCount--;
                long v = accessor.getValue(slot);
                shiftConflictingKeys(slot);
                return v;
            }
            slot = (slot + 1) & mask;
            if (slot == wrappedAround) {
                break;
            }
        }

        return missingValue;
    }

    @Override
    public boolean delete(Long key) {
        return delete(key.longValue());
    }

    @Override
    public boolean delete(long key) {
        return remove(key) != missingValue;
    }

    @Override
    public boolean remove(Object key, Object value) {
        return remove((long) (Long) key, (long) (Long) value);
    }

    @Override
    public boolean remove(long key, long value) {
        checkKeyAndValue(key, value);

        ensureMemory();

        final int mask = allocatedSlotCount - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (accessor.isAssigned(slot)) {
            if (accessor.getKey(slot) == key) {
                long current = accessor.getValue(slot);
                if (current == value) {
                    assignedSlotCount--;
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
     * Shift all the slot-conflicting keys allocated to (and including) <code>slot</code>.
     */
    private void shiftConflictingKeys(int slotCurr) {
        final int mask = allocatedSlotCount - 1;
        int slotPrev;
        while (true) {
            slotPrev = slotCurr;
            slotCurr = (slotCurr + 1) & mask;

            while (accessor.isAssigned(slotCurr)) {
                int slotOther = rehash(accessor.getKey(slotCurr), perturbation) & mask;
                if (slotPrev <= slotCurr) {
                    // We're on the right of the original slot.
                    if (slotPrev >= slotOther || slotOther > slotCurr) {
                        break;
                    }
                } else {
                    // We've wrapped around.
                    if (slotPrev >= slotOther && slotOther > slotCurr) {
                        break;
                    }
                }
                slotCurr = (slotCurr + 1) & mask;
            }

            if (!accessor.isAssigned(slotCurr)) {
                break;
            }

            // Shift key/value pair.
            accessor.setKey(slotPrev, accessor.getKey(slotCurr));
            accessor.setValue(slotPrev, accessor.getValue(slotCurr));
        }

        accessor.setKey(slotPrev, missingValue);
        accessor.setValue(slotPrev, missingValue);
    }

    @Override
    public boolean containsKey(Object key) {
        return containsKey((long) (Long) key);
    }

    @Override
    public boolean containsKey(long key) {
        checkKey(key);

        ensureMemory();

        final int mask = allocatedSlotCount - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (accessor.isAssigned(slot)) {
            if (accessor.getKey(slot) == key) {
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
    public boolean containsValue(Object value) {
        return containsValue((long) (Long) value);
    }

    @Override
    public boolean containsValue(long value) {
        checkValue(value);

        ensureMemory();

        for (int slot = 0; slot < allocatedSlotCount; slot++) {
            if (accessor.isAssigned(slot)) {
                long current = accessor.getValue(slot);
                if (current == value) {
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

        SlotIter(int startSlot) {
            nextSlot = advance(startSlot);
        }

        @Override
        public final int advance(int start) {
            ensureMemory();
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
            currentSlot = nextSlot;
            nextSlot = advance(nextSlot + 1);
            return currentSlot;
        }

        @Override
        public final void remove() {
            ensureMemory();

            if (currentSlot < 0) {
                throw new NoSuchElementException();
            }

            assignedSlotCount--;
            shiftConflictingKeys(currentSlot);

            // If current slot is assigned after removal and shift.
            // Then it means entry in the next slot is moved to current slot.
            if (accessor.isAssigned(currentSlot)) {
                nextSlot = currentSlot;
            }
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
    public Set<Long> keySet() {
        ensureMemory();
        return new KeySet();
    }

    private final class KeySet extends AbstractSet<Long> {

        public Iterator<Long> iterator() {
            return new KeyIter();
        }

        public int size() {
            return Long2LongElasticHashMap.this.size();
        }

        public boolean contains(Object o) {
            return containsKey(o);
        }

        public boolean remove(Object o) {
            return Long2LongElasticHashMap.this.remove(o) != missingValue;
        }

        public void clear() {
            Long2LongElasticHashMap.this.clear();
        }

    }

    private final class KeyIter extends SlotIter<Long> implements Iterator<Long> {

        KeyIter() {
        }

        KeyIter(int startSlot) {
            super(startSlot);
        }

        @Override
        public Long next() {
            nextSlot();
            return accessor.getKey(currentSlot);
        }

    }

    public final SlottableIterator<Long> keyIterator() {
        return new KeyIter();
    }

    public final SlottableIterator<Long> keyIterator(int slot) {
        return new KeyIter(slot);
    }

    @Override
    public Collection<Long> values() {
        ensureMemory();
        return new Values();
    }

    private final class Values extends AbstractCollection<Long> {

        public Iterator<Long> iterator() {
            return new ValueIter();
        }

        public int size() {
            return Long2LongElasticHashMap.this.size();
        }

        public boolean contains(Object o) {
            return containsValue(o);
        }

        public void clear() {
            Long2LongElasticHashMap.this.clear();
        }

    }

    public final SlottableIterator<Long> valueIterator() {
        return new ValueIter();
    }

    private final class ValueIter extends SlotIter<Long> implements Iterator<Long> {

        @Override
        public Long next() {
            nextSlot();
            return accessor.getValue(currentSlot);
        }

    }

    @Override
    public Set<Map.Entry<Long, Long>> entrySet() {
        ensureMemory();
        return new EntrySet();
    }

    private final class EntrySet extends AbstractSet<Map.Entry<Long, Long>> {

        public Iterator<Map.Entry<Long, Long>> iterator() {
            return new EntryIter();
        }

        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }
            Map.Entry<Long, Long> e = (Map.Entry<Long, Long>) o;
            Long value = get(e.getKey());
            if (value != missingValue) {
                if (value.equals(e.getValue())) {
                    return true;
                }
            }
            return false;
        }

        public boolean remove(Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }
            Map.Entry<Long, Long> e = (Map.Entry<Long, Long>) o;
            Long key = e.getKey();
            return delete(key);
        }

        public int size() {
            return Long2LongElasticHashMap.this.size();
        }

        public void clear() {
            Long2LongElasticHashMap.this.clear();
        }

    }

    public SlottableIterator<Map.Entry<Long, Long>> entryIterator() {
        return new EntryIter();
    }

    public SlottableIterator<Map.Entry<Long, Long>> entryIterator(int slot) {
        return new EntryIter(slot);
    }

    private final class EntryIter extends SlotIter<Map.Entry<Long, Long>> {

        EntryIter() { }

        EntryIter(int slot) {
            if (slot < 0 || slot > allocatedSlotCount) {
                slot = 0;
            }
            nextSlot = advance(slot);
        }

        @Override
        public Map.Entry<Long, Long> next() {
            nextSlot();
            return new MapEntry(currentSlot);
        }

    }

    private final class MapEntry implements Map.Entry<Long, Long> {

        private final int slot;

        private MapEntry(final int slot) {
            this.slot = slot;
        }

        @Override
        public Long getKey() {
            return accessor.getKey(slot);
        }

        @Override
        public Long getValue() {
            return accessor.getValue(slot);
        }

        @Override
        public Long setValue(Long value) {
            Long current = getValue();
            accessor.setValue(slot, value);
            return current;
        }

    }

    /**
     * Clears the map by removing and disposing all key/value pairs stored.
     */
    @Override
    public void clear() {
        ensureMemory();

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
        return "Long2LongElasticHashMap{address=" + accessor
                + ", allocated=" + allocatedSlotCount
                + ", assigned=" + assignedSlotCount
                + ", loadFactor=" + loadFactor
                + ", resizeAt=" + resizeAt
                + ", missingValue=" + missingValue + '}';
    }

}
