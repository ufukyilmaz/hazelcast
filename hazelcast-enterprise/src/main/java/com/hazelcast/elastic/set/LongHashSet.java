package com.hazelcast.elastic.set;

import com.hazelcast.elastic.LongIterator;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.util.HashUtil;
import com.hazelcast.util.QuickMath;
import sun.misc.Unsafe;

import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

/**
 * A hash set of <code>long</code>s, implemented using using open
 * addressing with linear probing for collision resolution.
 *
 * <p>
 * The internal buffers of this implementation ({@link #keys},
 * {@link #allocated}) are always allocated to the nearest size that is a power of two. When
 * the capacity exceeds the given load factor, the buffer size is doubled.
 * </p>
 *
 * @author This code is inspired by the collaboration and implementation in the <a
 *         href="http://fastutil.dsi.unimi.it/">fastutil</a> project.
 */
public class LongHashSet implements LongSet {

    private static final long allocationFactor = 9L;

    protected final MemoryAllocator malloc;

    private long baseAddress;

    /**
     * Hash-indexed array holding all keys.
     *
     */
    private long keys; // long[] keys

    /**
     * Information if an entry (slot) in the {@link #keys} table is allocated
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

    /**
     * Creates a hash map with the default capacity of {@value #DEFAULT_CAPACITY},
     * load factor of {@value #DEFAULT_LOAD_FACTOR}.
     */
    public LongHashSet(MemoryAllocator malloc) {
        this(DEFAULT_CAPACITY, malloc);
    }

    /**
     * Creates a hash map with the given initial capacity, default load factor of
     * {@value #DEFAULT_LOAD_FACTOR}.
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
    public boolean add(long key) {
        assert assigned < allocatedLength;

        final int mask = allocatedLength - 1;
        int slot = rehash(key, perturbation) & mask;
        while (isAllocated(slot)) {
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
            setAllocated(slot, true);
            setKey(slot, key);
        }
        return true;
    }

    /**
     * Expand the internal storage buffers (capacity) and rehash.
     */
    private void expandAndAdd(long pendingKey, int freeSlot) {
        assert assigned == resizeAt;
        assert !isAllocated(freeSlot);

        // Try to allocate new buffers first. If we OOM, it'll be now without
        // leaving the data structure in an inconsistent state.
        final long oldAddress = baseAddress;
        final long oldKeys = keys;
        final long oldAllocated = allocated;
        final int oldAllocatedLength = allocatedLength;

        allocateBuffers(nextCapacity(allocatedLength));

        // We have succeeded at allocating new data so insert the pending key/value at
        // the free slot in the temp arrays before rehashing.
        assigned++;
        writeBool(oldAllocated, freeSlot, true);
        writeLong(oldKeys, freeSlot, pendingKey);

        // Rehash all stored keys into the new buffers.
        final int mask = allocatedLength - 1;
        for (int i = oldAllocatedLength; --i >= 0; ) {
            if (readBool(oldAllocated, i)) {
                final long key = readLong(oldKeys, i);

                int slot = rehash(key, perturbation) & mask;
                while (isAllocated(slot)) {
                    slot = (slot + 1) & mask;
                }

                setAllocated(slot, true);
                setKey(slot, key);
            }
        }
        malloc.free(oldAddress, oldAllocatedLength * allocationFactor);
    }

    /**
     * Allocate internal buffers for a given capacity.
     *
     * @param capacity New capacity (must be a power of two).
     */

    private void allocateBuffers(int capacity) {
        long allocationCapacity = capacity * allocationFactor;
        baseAddress = malloc.allocate(allocationCapacity);
        UnsafeHelper.UNSAFE.setMemory(baseAddress, allocationCapacity, (byte) 0);

        keys = baseAddress;
        allocated = baseAddress + (capacity * 8L);

        allocatedLength = capacity;
        resizeAt = Math.max(2, (int) Math.ceil(capacity * loadFactor)) - 1;
        perturbation = computePerturbationValue(capacity);
    }

    @Override
    public boolean remove(long key) {
        final int mask = allocatedLength - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (isAllocated(slot)) {
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
        final int mask = allocatedLength - 1;
        int slotPrev, slotOther;
        while (true) {
            slotCurr = ((slotPrev = slotCurr) + 1) & mask;

            while (isAllocated(slotCurr)) {
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

            if (!isAllocated(slotCurr)) {
                break;
            }

            // Shift key
            setKey(slotPrev, getKey(slotCurr));
        }

        setAllocated(slotPrev, false);
        setKey(slotPrev, 0L);
    }

    @Override
    public boolean contains(long key) {
        final int mask = allocatedLength - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (isAllocated(slot)) {
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
            for (int slot = nextSlot + 1; slot < allocatedLength; slot++) {
                if (isAllocated(slot)) {
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
        if (baseAddress > 0L) {
            assigned = 0;
            Unsafe unsafe = UnsafeHelper.UNSAFE;
            unsafe.setMemory(keys, allocatedLength * 8L, (byte) 0);
            unsafe.setMemory(allocated, allocatedLength, (byte) 0);
        }
    }

    @Override
    public void destroy() {
        assigned = 0;
        if (baseAddress > 0L) {
            malloc.free(baseAddress, allocatedLength * allocationFactor);
        }
        allocatedLength = 0;
        baseAddress = -1L;
        keys = -1L;
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LongHashSet{");
        sb.append("baseAddress=").append(baseAddress);
        sb.append(", allocatedLength=").append(allocatedLength);
        sb.append(", assigned=").append(assigned);
        sb.append(", loadFactor=").append(loadFactor);
        sb.append(", resizeAt=").append(resizeAt);
        sb.append('}');
        return sb.toString();
    }
}
