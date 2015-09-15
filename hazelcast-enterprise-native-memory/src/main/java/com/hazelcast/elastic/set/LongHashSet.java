package com.hazelcast.elastic.set;

import com.hazelcast.elastic.CapacityUtil;
import com.hazelcast.elastic.LongIterator;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.util.HashUtil;
import sun.misc.Unsafe;

import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

import static com.hazelcast.elastic.CapacityUtil.DEFAULT_CAPACITY;
import static com.hazelcast.elastic.CapacityUtil.DEFAULT_LOAD_FACTOR;
import static com.hazelcast.elastic.CapacityUtil.MIN_CAPACITY;
import static com.hazelcast.elastic.CapacityUtil.nextCapacity;
import static com.hazelcast.elastic.CapacityUtil.roundCapacity;
/**
 * A hash set of <code>long</code>s, implemented using using open
 * addressing with linear probing for collision resolution.
 *
 * <p>
 * The internal buffer of this implementation
 * {@link #allocated}) is always allocated to the nearest size that is a power of two. When
 * the capacity exceeds the given load factor, the buffer size is doubled.
 * </p>
 *
 * @author This code is inspired by the collaboration and implementation in the <a
 *         href="http://fastutil.dsi.unimi.it/">fastutil</a> project.
 */
public class LongHashSet implements LongSet {

    private static final long ENTRY_LENGTH = 12L;

    private final MemoryAllocator malloc;

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
     *
     * @see "http://issues.carrot2.org/browse/HPPC-80"
     */
    private int perturbation;

    /**
     * Creates a hash map with the default capacity of {@value CapacityUtil#DEFAULT_CAPACITY},
     * load factor of {@value CapacityUtil#DEFAULT_LOAD_FACTOR}.
     */
    public LongHashSet(MemoryAllocator malloc) {
        this(DEFAULT_CAPACITY, malloc);
    }

    /**
     * Creates a hash map with the given initial capacity, default load factor of
     * {@value CapacityUtil#DEFAULT_LOAD_FACTOR}.
     *
     * @param initialCapacity Initial capacity (greater than zero and automatically
     *                        rounded to the next power of two).
     * @param malloc
     */
    public LongHashSet(int initialCapacity, MemoryAllocator malloc) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR, malloc);
    }

    /**
     * Creates a hash map with the given initial capacity,
     * load factor.
     *  @param initialCapacity Initial capacity (greater than zero and automatically
     *                        rounded to the next power of two).
     * @param loadFactor      The load factor (greater than zero and smaller than 1).
     * @param malloc
     */
    public LongHashSet(int initialCapacity, float loadFactor, MemoryAllocator malloc) {

        initialCapacity = Math.max(initialCapacity, MIN_CAPACITY);

        assert initialCapacity > 0
                : "Initial capacity must be between (0, " + Integer.MAX_VALUE + "].";
        assert loadFactor > 0 && loadFactor <= 1
                : "Load factor must be between (0, 1].";

        this.malloc = malloc;
        this.loadFactor = loadFactor;
        allocateBuffers(roundCapacity(initialCapacity));
    }

    @Override
    public boolean add(long key) {
        ensureMemory();
        assert assigned < allocated;

        final int mask = allocated - 1;
        int slot = rehash(key, perturbation) & mask;
        while (isAssigned(slot)) {
            long slotKey = getKey(slot);
            if (slotKey == key) {
                return false;
            }
            slot = (slot + 1) & mask;
        }

        // Check if we need to grow. If so, reallocate new data, fill in the last element
        // and rehash.
        if (assigned == resizeAt) {
            expandAndAdd(key, slot);
        } else {
            assigned++;
            setAssigned(slot, true);
            setKey(slot, key);
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
        setAssigned(oldAddress, freeSlot, true);
        setKey(oldAddress, freeSlot, pendingKey);

        // Rehash all stored keys into the new buffers.
        final int mask = allocated - 1;
        for (int slot = oldAllocated; --slot >= 0; ) {
            if (isAssigned(oldAddress, slot)) {
                long key = getKey(oldAddress, slot);

                int newSlot = rehash(key, perturbation) & mask;
                while (isAssigned(newSlot)) {
                    newSlot = (newSlot + 1) & mask;
                }

                setAssigned(newSlot, true);
                setKey(newSlot, key);
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
        UnsafeHelper.UNSAFE.setMemory(address, allocationCapacity, (byte) 0);

        allocated = capacity;
        resizeAt = Math.max(2, (int) Math.ceil(capacity * loadFactor)) - 1;
        perturbation = computePerturbationValue(capacity);
    }

    @Override
    public boolean remove(long key) {
        ensureMemory();
        final int mask = allocated - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (isAssigned(slot)) {
            long slotKey = getKey(slot);
            if (slotKey == key) {
                assigned--;
                shiftConflictingKeys(slot);
                return true;
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
        final int mask = allocated - 1;
        int slotPrev, slotOther;
        while (true) {
            slotCurr = ((slotPrev = slotCurr) + 1) & mask;

            while (isAssigned(slotCurr)) {
                slotOther = rehash(getKey(slotCurr), perturbation) & mask;

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

            if (!isAssigned(slotCurr)) {
                break;
            }

            // Shift key
            setKey(slotPrev, getKey(slotCurr));
        }

        setAssigned(slotPrev, false);
        setKey(slotPrev, 0L);
    }

    @Override
    public boolean contains(long key) {
        ensureMemory();
        final int mask = allocated - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (isAssigned(slot)) {
            long slotKey = getKey(slot);
            if (slotKey == key) {
                return true;
            }
            slot = (slot + 1) & mask;
            if (slot == wrappedAround) break;
        }
        return false;
    }

    @Override
    public LongIterator iterator() {
        ensureMemory();
        return new KeysIter();
    }

    private class KeysIter extends SlotIter {
        @Override
        public long next() {
            int slot = nextSlot();
            return getKey(slot);
        }
    }

    private abstract class SlotIter implements LongIterator {
        int nextSlot = -1;
        int currentSlot = -1;

        SlotIter() {
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

        final int nextSlot() {
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
        if (address > 0L) {
            assigned = 0;
            Unsafe unsafe = UnsafeHelper.UNSAFE;
            unsafe.setMemory(address, allocated * ENTRY_LENGTH, (byte) 0);
        }
    }

    @Override
    public void destroy() {
        assigned = 0;
        if (address > 0L) {
            malloc.free(address, allocated * ENTRY_LENGTH);
        }
        allocated = 0;
        address = -1L;
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

    private void setAssigned(int slot, boolean value) {
        setAssigned(address, slot, value);
    }

    protected final long getKey(int slot) {
        return getKey(address, slot);
    }

    private void setKey(int slot, long key) {
        setKey(address, slot, key);
    }

    private static boolean isAssigned(long address, int slot) {
        long offset = slot * ENTRY_LENGTH;
        return UnsafeHelper.UNSAFE.getByte(address + offset) != 0;
    }

    private static void setAssigned(long address, int slot, boolean value) {
        long offset = slot * ENTRY_LENGTH;
        UnsafeHelper.UNSAFE.putByte(address + offset, (byte) (value ? 1 : 0));
    }

    private static long getKey(long address, int slot) {
        long offset = slot * ENTRY_LENGTH + 4;
        return UnsafeHelper.UNSAFE.getLong(address + offset);
    }

    private static void setKey(long address, int slot, long key) {
        long offset = slot * ENTRY_LENGTH + 4;
        UnsafeHelper.UNSAFE.putLong(address + offset, key);
    }

    private static int rehash(long o, int p) {
        return (int) HashUtil.MurmurHash3_fmix(o ^ p);
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

    private void ensureMemory() {
        if (address < 0L) {
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
        sb.append('}');
        return sb.toString();
    }
}
