package com.hazelcast.elastic.map;

import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.util.HashUtil;
import com.hazelcast.util.QuickMath;

import static com.hazelcast.elastic.CapacityUtil.nextCapacity;
import static com.hazelcast.elastic.CapacityUtil.roundCapacity;
import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.util.HashUtil.MurmurHash3_fmix;

/**
 * Implementation of {@code RawMap} using a native memory block as backing array.
 *
 * @see InlineNativeMemoryMap
 */
public class InlineNativeMemoryMapImpl implements InlineNativeMemoryMap {

    private final static int KEY_1_OFFSET = 0;
    private final static int KEY_2_OFFSET = 8;
    private final static int VALUE_OFFSET = 16;

    /**
     * Length of value in bytes
     */
    private final int valueLength;

    /**
     * Length of entry in bytes
     */
    private final int entryLength;

    /**
     * Memory allocator
     */
    private final MemoryAllocator malloc;

    /**
     * Base address of backing memory region of this map
     */
    private long baseAddress;

    /**
     * Number of allocated slots
     */
    private long allocated;

    /**
     * Bit mask used to compute slot index.
     */
    private long mask;

    /**
     * Cached number of assigned slots in {@link #allocated}.
     */
    private long assigned;

    /**
     * The load factor for this map (fraction of allocated slots
     * before the buffers must be rehashed or reallocated).
     */
    private final float loadFactor;

    /**
     * Resize buffers when {@link #assigned} hits this value.
     */
    private long resizeAt;

    /**
     * Constructs a new {@code RawMapImpl} with default initial capacity and default load factor (0.6).
     * {@code valueLength} must be a factor of 8.
     *
     * @param malloc Memory allocator
     * @param valueLength Length of value in bytes
     */
    public InlineNativeMemoryMapImpl(MemoryAllocator malloc, int valueLength) {
        this(malloc, valueLength, 16, 0.6f);
    }

    /**
     * Constructs a new {@code RawMapImpl} with the given initial capacity and the load factor. {@code valueLength}
     * must be a factor of 8.
     *
     * @param malloc Memory allocator
     * @param valueLength Length of value in bytes
     * @param initialCapacity Initial capacity of map (will be rounded to closest power of 2, if not already)
     * @param loadFactor Load factor
     */
    public InlineNativeMemoryMapImpl(MemoryAllocator malloc, int valueLength, int initialCapacity, float loadFactor) {
        if (QuickMath.modPowerOfTwo(valueLength, 8) != 0) {
            throw new IllegalArgumentException("Value length should be factor of 8!");
        }
        this.valueLength = valueLength;
        this.entryLength = valueLength + VALUE_OFFSET;
        this.malloc = malloc;
        this.loadFactor = loadFactor;

        allocate(roundCapacity((int) (initialCapacity / loadFactor)));
    }

    private void allocate(long capacity) {
        long allocationCapacity = capacity * entryLength;
        baseAddress = malloc.allocate(allocationCapacity);
        UnsafeHelper.UNSAFE.setMemory(baseAddress, allocationCapacity, (byte) 0);

        allocated = capacity;
        mask = capacity - 1;
        resizeAt = Math.max(2, (int) Math.ceil(capacity * loadFactor)) - 1;
    }

    @Override
    public long put(long key1, long key2) {
        ensureLive();

        // Check if we need to grow. If so, reallocate new data and rehash.
        if (assigned == resizeAt) {
            expand();
        }

        long slot = hash(key1, key2);
        while (isAssigned(slot)) {
            long slotKey1 = getKey1(slot);
            long slotKey2 = getKey2(slot);

            if (slotKey1 == key1 && slotKey2 == key2) {
                return -toValueAddress(slot);
            }
            slot = (slot + 1) & mask;
        }

        assigned++;
        putKey(slot, key1, key2);
        return toValueAddress(slot);
    }

    /**
     * Expand the internal storage buffers (capacity) and rehash.
     */
    private void expand() {
        assert assigned == resizeAt;

        // Try to allocate new buffers first. If we OOM, it'll be now without
        // leaving the data structure in an inconsistent state.
        final long oldAddress = baseAddress;
        final long oldAllocated = allocated;

        allocate(nextCapacity(allocated));

        // Rehash all stored keys into the new buffers.
        for (long slot = oldAllocated; --slot >= 0; ) {
            if (isAssigned(oldAddress, slot, entryLength)) {
                long keyAddress = getKey1(oldAddress, slot, entryLength);
                long keySequence = getKey2(oldAddress, slot, entryLength);
                long valueAddress = toValueAddress(oldAddress, slot, entryLength);

                long newSlot = hash(keyAddress, keySequence);
                while (isAssigned(newSlot)) {
                    newSlot = (newSlot + 1) & mask;
                }

                putKey(newSlot, keyAddress, keySequence);
                UnsafeHelper.UNSAFE.copyMemory(valueAddress, toValueAddress(newSlot), valueLength);
            }
        }
        malloc.free(oldAddress, oldAllocated * entryLength);
    }

    @Override
    public long get(long key1, long key2) {
        ensureLive();

        long slot = hash(key1, key2);
        final long wrappedAround = slot;

        while (isAssigned(slot)) {
            long slotAddress = getKey1(slot);
            long slotSequence = getKey2(slot);

            if (slotAddress == key1 && slotSequence == key2) {
                return toValueAddress(slot);
            }
            slot = (slot + 1) & mask;
            if (slot == wrappedAround) {
                break;
            }
        }
        return NULL_ADDRESS;
    }

    @Override
    public boolean remove(long key1, long key2) {
        ensureLive();
        long slot = hash(key1, key2);
        final long wrappedAround = slot;
        while (isAssigned(slot)) {
            long slotKey1 = getKey1(slot);
            long slotKey2 = getKey2(slot);

            if (slotKey1 == key1 && slotKey2 == key2) {
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
    private void shiftConflictingKeys(long slotCurr) {
        long slotPrev, slotOther;
        while (true) {
            slotCurr = ((slotPrev = slotCurr) + 1) & mask;

            while (isAssigned(slotCurr)) {
                slotOther = hash(getKey1(slotCurr), getKey2(slotCurr));

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

            // Shift key/value pair.
            putKey(slotPrev, getKey1(slotCurr), getKey2(slotCurr));
            UnsafeHelper.UNSAFE.copyMemory(toValueAddress(slotCurr), toValueAddress(slotPrev), valueLength);
        }

        putKey(slotPrev, 0L, 0L);
        UnsafeHelper.UNSAFE.setMemory(toValueAddress(slotPrev), valueLength, (byte) 0);
    }

    private long hash(long key1, long key2) {
        return MurmurHash3_fmix(key1 ^ key2) & mask;
    }

    @Override
    public long size() {
        return assigned;
    }

    @Override
    public void clear() {
        ensureLive();
        UnsafeHelper.UNSAFE.setMemory(baseAddress, allocated * entryLength, (byte) 0);
        assigned = 0;
    }

    @Override
    public void dispose() {
        if (baseAddress <= 0L) {
            return;
        }
        malloc.free(baseAddress, allocated * entryLength);
        baseAddress = -1L;
        allocated = 0;
        mask = 0;
        resizeAt = 0;
        assigned = 0;
    }

    @Override
    public int keyLength() {
        return 16;
    }

    @Override
    public int valueLength() {
        return valueLength;
    }

    @Override public InmmCursor cursor() {
        return new Cursor();
    }

    private boolean isAssigned(long slot) {
        return isAssigned(baseAddress, slot, entryLength);
    }

    private static boolean isAssigned(long baseAddr, long slot, int entryLength) {
        long offset = slot * entryLength + KEY_1_OFFSET;
        return UnsafeHelper.UNSAFE.getLong(baseAddr + offset) != 0L;
    }

    private long getKey1(long slot) {
        return getKey1(baseAddress, slot, entryLength);
    }

    private static long getKey1(long baseAddr, long slot, int entryLength) {
        long offset = slot * entryLength + KEY_1_OFFSET;
        return UnsafeHelper.UNSAFE.getLong(baseAddr + offset);
    }

    private long getKey2(long slot) {
        return getKey2(baseAddress, slot, entryLength);
    }

    private static long getKey2(long baseAddr, long slot, int entryLength) {
        long offset = slot * entryLength + KEY_2_OFFSET;
        return UnsafeHelper.UNSAFE.getLong(baseAddr + offset);
    }

    private void putKey(long slot, long key1, long key2) {
        putKey(baseAddress, slot, entryLength, key1, key2);
    }

    private static void putKey(long baseAddr, long slot, int entryLength, long key1, long key2) {
        long offset = slot * entryLength + KEY_1_OFFSET;
        UnsafeHelper.UNSAFE.putLong(baseAddr + offset, key1);

        offset += KEY_2_OFFSET;
        UnsafeHelper.UNSAFE.putLong(baseAddr + offset, key2);
    }

    private long toValueAddress(long slot) {
        return toValueAddress(baseAddress, slot, entryLength);
    }

    private static long toValueAddress(long baseAddr, long slot, int entryLength) {
        long offset = slot * entryLength + VALUE_OFFSET;
        return baseAddr + offset;
    }

    private void ensureLive() {
        if (baseAddress <= 0L) {
            throw new IllegalStateException("Map is already disposed!");
        }
    }

    private class Cursor implements InmmCursor {

        private long currentSlot = -1L;

        @Override public boolean advance() {
            ensureLive();
            if (currentSlot == Long.MIN_VALUE) {
                throw new IllegalStateException("Cursor is invalid!");
            }

            if (tryAdvance()) {
                return true;
            }

            currentSlot = Long.MIN_VALUE;
            return false;
        }

        private boolean tryAdvance() {
            for (long slot = currentSlot + 1; slot < allocated; slot++) {
                if (isAssigned(slot)) {
                    currentSlot = slot;
                    return true;
                }
            }
            return false;
        }

        @Override public long key1() {
            ensureValid();
            return getKey1(currentSlot);
        }

        @Override public long key2() {
            ensureValid();
            return getKey2(currentSlot);
        }

        @Override public long valueAddress() {
            ensureValid();
            return toValueAddress(currentSlot);
        }

        @Override public void remove() {
            ensureValid();

            assigned--;
            shiftConflictingKeys(currentSlot);

            // if current slot is assigned after
            // removal and shift
            // then it means entry in the next slot
            // is moved to current slot
            if (isAssigned(currentSlot)) {
                currentSlot--;
            }
        }

        private void ensureValid() {
            ensureLive();
            if (currentSlot < 0) {
                throw new IllegalStateException("Cursor is invalid!");
            }
        }
    }
}
