package com.hazelcast.elastic.set;

import com.hazelcast.elastic.LongIterator;
import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry;
import com.hazelcast.internal.memory.GlobalMemoryAccessorType;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.util.HashUtil;

import java.util.NoSuchElementException;

import static com.hazelcast.spi.impl.hashslot.CapacityUtil.DEFAULT_CAPACITY;
import static com.hazelcast.spi.impl.hashslot.CapacityUtil.DEFAULT_LOAD_FACTOR;
import static com.hazelcast.spi.impl.hashslot.CapacityUtil.MIN_CAPACITY;
import static com.hazelcast.spi.impl.hashslot.CapacityUtil.nextCapacity;
import static com.hazelcast.spi.impl.hashslot.CapacityUtil.roundCapacity;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.util.HashUtil.computePerturbationValue;

/**
 * A hash set of <code>long</code>s, implemented using using open
 * addressing with linear probing for collision resolution.
 *
 * <p>
 * The internal buffer of this implementation
 * {@link #allocated}) is always allocated to the nearest size that is a power of two. When
 * the capacity exceeds the given load factor, the buffer size is doubled.
 * </p>
 */
public class LongHashSet implements LongSet {

    // We are using `STANDARD` memory accessor because we internally guarantee that
    // every memory access is aligned.
    private static final MemoryAccessor MEMORY_ACCESSOR =
            GlobalMemoryAccessorRegistry.getGlobalMemoryAccessor(GlobalMemoryAccessorType.STANDARD);

    private static final long ENTRY_LENGTH = 8L;

    private final MemoryAllocator malloc;

    private final long nullItem;

    private long address;

    private int allocated;

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
     * Resize buffers when {@link #allocated} hits this value.
     */
    private int resizeAt;

    /**
     * We perturb hashed values with the array size to avoid problems with
     * nearly-sorted-by-hash values on iterations.
     */
    private int perturbation;

    /**
     * Creates a hash map with the default capacity of
     * {@value com.hazelcast.spi.impl.hashslot.CapacityUtil#DEFAULT_CAPACITY},
     * load factor of {@value com.hazelcast.spi.impl.hashslot.CapacityUtil#DEFAULT_LOAD_FACTOR}.
     */
    public LongHashSet(MemoryAllocator malloc, long nullItem) {
        this(DEFAULT_CAPACITY, malloc, nullItem);
    }

    /**
     * Creates a hash map with the given initial capacity, default load factor of
     * {@value com.hazelcast.spi.impl.hashslot.CapacityUtil#DEFAULT_LOAD_FACTOR}.
     *  @param initialCapacity Initial capacity (greater than zero and automatically
     *                        rounded to the next power of two).
     */
    public LongHashSet(int initialCapacity, MemoryAllocator malloc, long nullItem) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR, malloc, nullItem);
    }

    /**
     * Creates a hash map with the given initial capacity,
     * load factor.
     * @param initialCapacity Initial capacity (greater than zero and automatically
     *                        rounded to the next power of two).
     * @param loadFactor      The load factor (greater than zero and smaller than 1).
     */
    public LongHashSet(int initialCapacity, float loadFactor, MemoryAllocator malloc, long nullItem) {
        initialCapacity = Math.max(initialCapacity, MIN_CAPACITY);

        assert initialCapacity > 0
                : "Initial capacity must be between (0, " + Integer.MAX_VALUE + "].";
        assert loadFactor > 0 && loadFactor <= 1
                : "Load factor must be between (0, 1].";

        this.malloc = malloc;
        this.loadFactor = loadFactor;
        this.nullItem = nullItem;
        allocateBuffers(roundCapacity(initialCapacity));
    }

    @Override
    public boolean add(long value) {
        ensureMemory();
        assert assigned < allocated;

        if (value == nullItem) {
            throw new IllegalArgumentException("Can't add null-item!");
        }

        final int mask = allocated - 1;
        int slot = rehash(value, perturbation) & mask;
        while (isAssigned(slot)) {
            long slotItem = getItem(slot);
            if (slotItem == value) {
                return false;
            }
            slot = (slot + 1) & mask;
        }

        // Check if we need to grow. If so, reallocate new data, fill in the last element
        // and rehash.
        if (assigned == resizeAt) {
            expandAndAdd(value, slot);
        } else {
            assigned++;
            setItem(slot, value);
        }
        return true;
    }

    /**
     * Expand the internal storage buffers (capacity) and rehash.
     */
    private void expandAndAdd(long pendingKey, int freeSlot) {
        assert assigned == resizeAt;
        assert !isAssigned(freeSlot);

        // Try to allocate new buffers first. If we OOM, it'll be now without
        // leaving the data structure in an inconsistent state.
        final long oldAddress = address;
        final int oldAllocated = allocated;

        allocateBuffers(nextCapacity(allocated));

        // We have succeeded at allocating new data so insert the pending key/value at
        // the free slot in the temp arrays before rehashing.
        assigned++;
        setItem(oldAddress, freeSlot, pendingKey);

        // Rehash all stored keys into the new buffers.
        final int mask = allocated - 1;
        for (int slot = oldAllocated; --slot >= 0;) {
            if (isAssigned(oldAddress, slot)) {
                long key = getItem(oldAddress, slot);

                int newSlot = rehash(key, perturbation) & mask;
                while (isAssigned(newSlot)) {
                    newSlot = (newSlot + 1) & mask;
                }

                setItem(newSlot, key);
            }
        }
        malloc.free(oldAddress, oldAllocated * ENTRY_LENGTH);
    }

    /**
     * Allocate internal buffers for a given capacity.
     *
     * @param capacity New capacity (must be a power of two).
     */

    private void allocateBuffers(int capacity) {
        long allocationCapacity = capacity * ENTRY_LENGTH;
        address = malloc.allocate(allocationCapacity);
        for (int i = 0; i < capacity; i++) {
            setItem(i, nullItem);
        }

        allocated = capacity;
        resizeAt = Math.max(2, (int) Math.ceil(capacity * loadFactor)) - 1;
        perturbation = computePerturbationValue(capacity);
    }

    @Override
    public boolean remove(long value) {
        ensureMemory();
        final int mask = allocated - 1;
        int slot = rehash(value, perturbation) & mask;
        final int wrappedAround = slot;
        while (isAssigned(slot)) {
            long slotItem = getItem(slot);
            if (slotItem == value) {
                assigned--;
                shiftConflictingKeys(slot);
                return true;
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
        final int mask = allocated - 1;
        int slotPrev;
        while (true) {
            slotPrev = slotCurr;
            slotCurr = (slotCurr + 1) & mask;

            while (isAssigned(slotCurr)) {
                int slotOther = rehash(getItem(slotCurr), perturbation) & mask;

                if (slotPrev <= slotCurr) {
                    // we're on the right of the original slot.
                    if (slotPrev >= slotOther || slotOther > slotCurr) {
                        break;
                    }
                } else {
                    // we've wrapped around.
                    if (slotPrev >= slotOther && slotOther > slotCurr) {
                        break;
                    }
                }
                slotCurr = (slotCurr + 1) & mask;
            }

            if (!isAssigned(slotCurr)) {
                break;
            }

            // Shift key
            setItem(slotPrev, getItem(slotCurr));
        }

        setItem(slotPrev, nullItem);
    }

    @Override
    public boolean contains(long key) {
        ensureMemory();
        final int mask = allocated - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (isAssigned(slot)) {
            long slotItem = getItem(slot);
            if (slotItem == key) {
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
    public LongIterator iterator() {
        ensureMemory();
        return new Iter();
    }

    private class Iter implements LongIterator {
        int nextSlot = -1;
        int currentSlot = -1;

        Iter() {
            nextSlot = advance();
        }

        private int advance() {
            ensureMemory();
            for (int slot = nextSlot + 1; slot < allocated; slot++) {
                if (isAssigned(slot)) {
                    return slot;
                }
            }
            return -1;
        }

        public final boolean hasNext() {
            return nextSlot > -1;
        }

        @Override
        public long next() {
            int slot = nextSlot();
            return getItem(slot);
        }

        private int nextSlot() {
            if (nextSlot < 0) {
                throw new NoSuchElementException();
            }
            currentSlot = nextSlot;
            nextSlot = advance();
            return currentSlot;
        }

        public final void remove() {
            if (currentSlot < 0) {
                throw new NoSuchElementException();
            }
            assigned--;
            shiftConflictingKeys(currentSlot);

            // if current slot is assigned after
            // removal and shift
            // then it means entry in the next slot
            // is moved to current slot
            if (isAssigned(currentSlot)) {
                nextSlot = currentSlot;
            }
        }

        @Override
        public void reset() {
            nextSlot = -1;
            currentSlot = -1;
            nextSlot = advance();
        }
    }

    /**
     * <p>Does not release internal buffers.</p>
     */
    @Override
    public void clear() {
        ensureMemory();
        if (address != NULL_ADDRESS) {
            assigned = 0;
            for (int i = 0; i < allocated; i++) {
                setItem(i, nullItem);
            }
        }
    }

    @Override
    public void dispose() {
        assigned = 0;
        if (address != NULL_ADDRESS) {
            malloc.free(address, allocated * ENTRY_LENGTH);
        }
        allocated = 0;
        address = NULL_ADDRESS;
        allocated = -1;
        resizeAt = 0;
    }

    @Override
    public int size() {
        return assigned;
    }

    public int capacity() {
        return allocated;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    protected final boolean isAssigned(int slot) {
        return isAssigned(address, slot);
    }

    protected final long getItem(int slot) {
        return getItem(address, slot);
    }

    private void setItem(int slot, long key) {
        setItem(address, slot, key);
    }

    private static boolean isAssigned(long address, int slot) {
        long offset = slot * ENTRY_LENGTH;
        return MEMORY_ACCESSOR.getLong(address + offset) != 0L;
    }

    private static long getItem(long address, int slot) {
        long offset = slot * ENTRY_LENGTH;
        return MEMORY_ACCESSOR.getLong(address + offset);
    }

    private static void setItem(long address, int slot, long key) {
        long offset = slot * ENTRY_LENGTH;
        MEMORY_ACCESSOR.putLong(address + offset, key);
    }

    private static int rehash(long o, int p) {
        return (int) HashUtil.MurmurHash3_fmix(o ^ p);
    }


    private void ensureMemory() {
        if (address == NULL_ADDRESS) {
            throw new IllegalStateException("Set is already destroyed!");
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LongHashSet{");
        sb.append("address=").append(address);
        sb.append(", allocated=").append(allocated);
        sb.append(", assigned=").append(assigned);
        sb.append(", loadFactor=").append(loadFactor);
        sb.append(", resizeAt=").append(resizeAt);
        sb.append(", nullItem=").append(nullItem);
        sb.append('}');
        return sb.toString();
    }
}
