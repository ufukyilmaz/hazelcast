package com.hazelcast.elastic.map;

import com.hazelcast.memory.MemoryAllocator;

import static com.hazelcast.elastic.CapacityUtil.DEFAULT_CAPACITY;
import static com.hazelcast.elastic.CapacityUtil.DEFAULT_LOAD_FACTOR;
import static com.hazelcast.elastic.CapacityUtil.nextCapacity;
import static com.hazelcast.elastic.CapacityUtil.roundCapacity;
import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.nio.UnsafeHelper.UNSAFE;
import static com.hazelcast.util.HashUtil.fastLongMix;
import static com.hazelcast.util.QuickMath.modPowerOfTwo;

/**
 * Implementation of {@link HashSlotArray} using a native memory block.
 */
public class HashSlotArrayImpl implements HashSlotArray {

    public static final long NULL_KEY = 0L;

    private static final int KEY_1_OFFSET = 0;
    private static final int KEY_2_OFFSET = 8;
    private static final int KEY_LENGTH = 16;
    private static final int VALUE_OFFSET = 16;
    private static final int VALUE_LENGTH_GRANULARITY = 8;

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
    private long capacity;

    /**
     * Bit mask used to compute slot index.
     */
    private long mask;

    /**
     * Cached number of assigned slots in {@link #capacity}.
     */
    private long size;

    /**
     * The load factor for this map (fraction of allocated slots
     * before the buffers must be rehashed or reallocated).
     */
    private final float loadFactor;

    /**
     * Resize buffers when {@link #size} hits this value.
     */
    private long expandAt;

    /**
     * Constructs a new {@code HashSlotArrayImpl} with default initial capacity
     * ({@link com.hazelcast.elastic.CapacityUtil#DEFAULT_CAPACITY})
     * and default load factor ({@link com.hazelcast.elastic.CapacityUtil#DEFAULT_LOAD_FACTOR}).
     * {@code valueLength} must be a factor of 8.
     *
     * @param malloc Memory allocator
     * @param valueLength Length of value in bytes
     */
    public HashSlotArrayImpl(MemoryAllocator malloc, int valueLength) {
        this(malloc, valueLength, DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Constructs a new {@code HashSlotArrayImpl} with the given initial capacity and the load factor.
     * {@code valueLength} must be a factor of 8.
     *
     * @param malloc Memory allocator
     * @param valueLength Length of value in bytes
     * @param initialCapacity Initial capacity of map (will be rounded to closest power of 2, if not already)
     * @param loadFactor Load factor
     */
    public HashSlotArrayImpl(MemoryAllocator malloc, int valueLength, int initialCapacity, float loadFactor) {
        if (modPowerOfTwo(valueLength, VALUE_LENGTH_GRANULARITY) != 0) {
            throw new IllegalArgumentException("Value length should be factor of 8!");
        }
        this.valueLength = valueLength;
        this.entryLength = KEY_LENGTH + valueLength;
        this.malloc = malloc;
        this.loadFactor = loadFactor;

        allocateArrayAndAdjustFields(roundCapacity((int) (initialCapacity / loadFactor)));
    }

    @Override
    public final long ensure(long key1, long key2) {
        assert key1 != NULL_KEY : "ensure called with key1 == " + NULL_KEY;
        ensureLive();
        // Check if we need to grow. If so, reallocate new data and rehash.
        if (size == expandAt) {
            resizeTo(nextCapacity(capacity));
        }
        long slot = hash(key1, key2);
        while (isAssigned(slot)) {
            long slotKey1 = getKey1(slot);
            long slotKey2 = getKey2(slot);
            if (slotKey1 == key1 && slotKey2 == key2) {
                return -getValueAddress(slot);
            }
            slot = (slot + 1) & mask;
        }
        size++;
        putKey(slot, key1, key2);
        return getValueAddress(slot);
    }

    @Override
    public final long get(long key1, long key2) {
        assert key1 != NULL_KEY : "get called with key1 == " + NULL_KEY;
        ensureLive();
        long slot = hash(key1, key2);
        final long wrappedAround = slot;
        while (isAssigned(slot)) {
            long slotAddress = getKey1(slot);
            long slotSequence = getKey2(slot);
            if (slotAddress == key1 && slotSequence == key2) {
                return getValueAddress(slot);
            }
            slot = (slot + 1) & mask;
            if (slot == wrappedAround) {
                break;
            }
        }
        return NULL_ADDRESS;
    }

    @Override
    public final boolean remove(long key1, long key2) {
        assert key1 != NULL_KEY : "remove called with key1 == " + NULL_KEY;
        ensureLive();
        long slot = hash(key1, key2);
        final long wrappedAround = slot;
        while (isAssigned(slot)) {
            long slotKey1 = getKey1(slot);
            long slotKey2 = getKey2(slot);
            if (slotKey1 == key1 && slotKey2 == key2) {
                size--;
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

    protected long hash(long key1, long key2) {
        return fastLongMix(fastLongMix(key1) + key2) & mask;
    }

    @Override
    public final long size() {
        return size;
    }

    @Override
    public final void clear() {
        ensureLive();
        UNSAFE.setMemory(baseAddress, capacity * entryLength, (byte) 0);
        size = 0;
    }

    @Override public final boolean trimToSize() {
        final long minCapacity = minCapacityForSize(size, loadFactor);
        if (capacity <= minCapacity) {
            return false;
        }
        resizeTo(minCapacity);
        assert expandAt >= size : String.format(
                "trimToSize() shrunk the capacity to %,d and expandAt to %,d, which is less than the current size %,d",
                capacity, expandAt, size);
        return true;
    }

    @Override
    public final void dispose() {
        if (baseAddress <= 0L) {
            return;
        }
        malloc.free(baseAddress, capacity * entryLength);
        baseAddress = -1L;
        capacity = 0;
        mask = 0;
        expandAt = 0;
        size = 0;
    }

    @Override public final HashSlotCursor cursor() {
        return new Cursor();
    }

    private boolean isAssigned(long slot) {
        return isAssigned(baseAddress, slot);
    }

    private long getKey1(long slot) {
        return key1At(slotBase(baseAddress, slot));
    }

    private long getKey2(long slot) {
        return key2At(slotBase(baseAddress, slot));
    }

    private long getValueAddress(long slot) {
        return addrOfValueAt(slotBase(baseAddress, slot));
    }

    private void putKey(long slot, long key1, long key2) {
        final long slotBase = slotBase(baseAddress, slot);
        UNSAFE.putLong(slotBase + KEY_1_OFFSET, key1);
        UNSAFE.putLong(slotBase + KEY_2_OFFSET, key2);
    }

    private boolean isAssigned(long baseAddr, long slot) {
        return key1At(slotBase(baseAddr, slot)) != NULL_KEY;
    }

    public static long key1At(long slotBaseAddr) {
        return UNSAFE.getLong(slotBaseAddr + KEY_1_OFFSET);
    }

    public static long key2At(long slotBaseAddr) {
        return UNSAFE.getLong(slotBaseAddr + KEY_2_OFFSET);
    }

    public static long addrOfValueAt(long slotBaseAddr) {
        return slotBaseAddr + VALUE_OFFSET;
    }

    public static long valueAddr2slotBase(long valueAddr) {
        return valueAddr - VALUE_OFFSET;
    }

    private long slotBase(long baseAddr, long slot) {
        return baseAddr + entryLength * slot;
    }

    private void ensureLive() {
        if (baseAddress <= 0L) {
            throw new IllegalStateException("Map is already disposed!");
        }
    }

    private void allocateArrayAndAdjustFields(long newCapacity) {
        long allocatedSize = newCapacity * entryLength;
        baseAddress = malloc.allocate(allocatedSize);
        UNSAFE.setMemory(baseAddress, allocatedSize, (byte) 0);
        capacity = newCapacity;
        mask = newCapacity - 1;
        expandAt = maxSizeForCapacity(newCapacity, loadFactor);
    }

    private static long maxSizeForCapacity(long capacity, float loadFactor) {
        return Math.max(2, (long) Math.ceil(capacity * loadFactor)) - 1;
    }

    private static long minCapacityForSize(long size, float loadFactor) {
        return roundCapacity((long) Math.ceil(size / loadFactor));
    }

    /**
     * Shift all the slot-conflicting keys allocated to (and including) <code>slot</code>.
     */
    @SuppressWarnings("checkstyle:innerassignment")
    private void shiftConflictingKeys(long slotCurr) {
        long slotPrev;
        long slotOther;
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
            UNSAFE.copyMemory(getValueAddress(slotCurr), getValueAddress(slotPrev), valueLength);
        }
        putKey(slotPrev, 0L, 0L);
        UNSAFE.setMemory(getValueAddress(slotPrev), valueLength, (byte) 0);
    }

    /**
     * Allocate a new slot array with the requested size and move all the
     * assigned slots from the current array into the new one.
     */
    private void resizeTo(long newCapacity) {
        // Allocate new array first, ensuring that the possible OOME
        // does not ruin the consistency of the existing data structure.
        final long oldAddress = baseAddress;
        final long oldCapacity = capacity;
        allocateArrayAndAdjustFields(newCapacity);
        // Put the assigned slots into the new array.
        for (long slot = oldCapacity; --slot >= 0;) {
            if (isAssigned(oldAddress, slot)) {
                long key1 = key1At(slotBase(oldAddress, slot));
                long key2 = key2At(slotBase(oldAddress, slot));
                long valueAddress = addrOfValueAt(slotBase(oldAddress, slot));
                long newSlot = hash(key1, key2);
                while (isAssigned(newSlot)) {
                    newSlot = (newSlot + 1) & mask;
                }
                putKey(newSlot, key1, key2);
                UNSAFE.copyMemory(valueAddress, getValueAddress(newSlot), valueLength);
            }
        }
        malloc.free(oldAddress, oldCapacity * entryLength);
    }

    private class Cursor implements HashSlotCursor {

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
            for (long slot = currentSlot + 1; slot < capacity; slot++) {
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
            return getValueAddress(currentSlot);
        }

        @Override public void remove() {
            ensureValid();
            size--;
            shiftConflictingKeys(currentSlot);
            // if the current slot ended up assigned after removal and shift,
            // it means that the entry in the next slot was moved to the current slot
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
