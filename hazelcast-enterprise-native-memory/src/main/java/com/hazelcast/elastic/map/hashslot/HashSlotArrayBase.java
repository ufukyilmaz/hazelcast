package com.hazelcast.elastic.map.hashslot;

import com.hazelcast.memory.MemoryAllocator;

import static com.hazelcast.elastic.CapacityUtil.DEFAULT_LOAD_FACTOR;
import static com.hazelcast.elastic.CapacityUtil.nextCapacity;
import static com.hazelcast.elastic.CapacityUtil.roundCapacity;
import static com.hazelcast.internal.memory.MemoryAccessor.AMEM;
import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.util.HashUtil.fastLongMix;
import static com.hazelcast.util.QuickMath.modPowerOfTwo;

/**
 * Common implementation base for {@link HashSlotArray} and {@link HashSlotArrayTwinKey}.
 */
abstract class HashSlotArrayBase {

    protected static final int KEY_1_OFFSET = 0;
    private static final int KEY_2_OFFSET = 8;
    private static final int VALUE_LENGTH_GRANULARITY = 8;

    /**
     * Base address of the backing memory region of this hash slot array.
     */
    protected long baseAddress;

    /**
     * Sentinel value that marks a slot as "unassigned".
     */
    protected final long unassignedSentinel;

    /**
     * Offset (from the slot's base address) where the unassigned sentinel value is to be found.
     */
    protected final long offsetOfUnassignedSentinel;

    private final MemoryAllocator malloc;

    /**
     * Total length of an array slot in bytes.
     */
    private final int slotLength;

    /**
     * Offset of the value block from the slot's base address.
     */
    private final int valueOffset;

    /**
     * Length of the value block in bytes.
     */
    private final int valueLength;

    /**
     * Capacity (in terms of slots) of the currently allocated memory region.
     */
    private long capacity;

    /**
     * Bit mask used to compute the slot index.
     */
    private long mask;

    /**
     * Number of assigned hash slots.
     */
    private long size;

    /**
     * The maximum load factor ({@link #size} / {@link #capacity}) for this hash slot array.
     * The array will be expanded as needed to enforce this limit.
     */
    private final float loadFactor;

    /**
     * Resize buffers when {@link #size} hits this value.
     */
    private long expandAt;

    /**
     * Constructs a new {@code HashSlotArrayImpl} with the given initial capacity and the load factor.
     * {@code valueLength} must be a factor of 8.
     *
     * @param unassignedSentinel the value to be used to mark an unassigned slot
     * @param offsetOfUnassignedSentinel offset (from each slot's base address) where the unassigned sentinel is kept
     * @param malloc memory allocator
     * @param keyLength length of key in bytes
     * @param valueLength length of value in bytes
     * @param initialCapacity Initial capacity of map (will be rounded to closest power of 2, if not already)
     */
    protected HashSlotArrayBase(long unassignedSentinel, long offsetOfUnassignedSentinel, MemoryAllocator malloc,
                                int keyLength, int valueLength, int initialCapacity
    ) {
        assert modPowerOfTwo(valueLength, VALUE_LENGTH_GRANULARITY) == 0
                : "Value length should be a positive multiple of 8";
        this.unassignedSentinel = unassignedSentinel;
        this.offsetOfUnassignedSentinel = offsetOfUnassignedSentinel;
        this.malloc = malloc;
        this.valueOffset = keyLength;
        this.valueLength = valueLength;
        this.slotLength = keyLength + valueLength;
        this.loadFactor = DEFAULT_LOAD_FACTOR;

        allocateArrayAndAdjustFields(roundCapacity((int) (initialCapacity / loadFactor)));
    }


    // These public final methods will automatically fit as interface implementation in subclasses

    public final long size() {
        return size;
    }

    public final void clear() {
        ensureLive();
        markAllUnassigned();
        size = 0;
    }

    public final boolean trimToSize() {
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

    public final void dispose() {
        if (baseAddress <= 0L) {
            return;
        }
        malloc.free(baseAddress, capacity * slotLength);
        baseAddress = -1L;
        capacity = 0;
        mask = 0;
        expandAt = 0;
        size = 0;
    }


    // These protected final methods will be called from the subclasses

    protected final long slotBase(long baseAddr, long slot) {
        return baseAddr + slotLength * slot;
    }

    protected final long ensure0(long key1, long key2) {
        ensureLive();
        // Check if we need to grow. If so, reallocate new data and rehash.
        if (size == expandAt) {
            resizeTo(nextCapacity(capacity));
        }
        long slot = maskedHash(key1, key2);
        while (isAssigned(slot)) {
            long slotKey1 = key1OfSlot(slot);
            long slotKey2 = key2OfSlot(slot);
            if (slotKey1 == key1 && slotKey2 == key2) {
                return -valueAddrOfSlot(slot);
            }
            slot = (slot + 1) & mask;
        }
        size++;
        putKey(slot, key1, key2);
        return valueAddrOfSlot(slot);
    }

    protected final long get0(long key1, long key2) {
        ensureLive();
        long slot = maskedHash(key1, key2);
        final long wrappedAround = slot;
        while (isAssigned(slot)) {
            long slotAddress = key1OfSlot(slot);
            long slotSequence = key2OfSlot(slot);
            if (slotAddress == key1 && slotSequence == key2) {
                return valueAddrOfSlot(slot);
            }
            slot = (slot + 1) & mask;
            if (slot == wrappedAround) {
                break;
            }
        }
        return NULL_ADDRESS;
    }

    protected final boolean remove0(long key1, long key2) {
        ensureLive();
        long slot = maskedHash(key1, key2);
        final long wrappedAround = slot;
        while (isAssigned(slot)) {
            long slotKey1 = key1OfSlot(slot);
            long slotKey2 = key2OfSlot(slot);
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

    /**
     * Shift all the slot-conflicting keys allocated to (and including) <code>slot</code>.
     */
    @SuppressWarnings("checkstyle:innerassignment")
    protected final void shiftConflictingKeys(long slotCurr) {
        long slotPrev;
        long slotOther;
        while (true) {
            slotCurr = ((slotPrev = slotCurr) + 1) & mask;
            while (isAssigned(slotCurr)) {
                slotOther = maskedHash(key1OfSlot(slotCurr), key2OfSlot(slotCurr));
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
            putKey(slotPrev, key1OfSlot(slotCurr), key2OfSlot(slotCurr));
            AMEM.copyMemory(valueAddrOfSlot(slotCurr), valueAddrOfSlot(slotPrev), valueLength);
        }
        final long slotBase = slotBase(slotPrev);
        AMEM.setMemory(slotBase, slotLength, (byte) 0);
        AMEM.putLong(slotBase + offsetOfUnassignedSentinel, unassignedSentinel);
    }

    protected final void ensureLive() {
        if (baseAddress <= 0L) {
            throw new IllegalStateException("Map is already disposed!");
        }
    }

    protected final long slotBase(long slot) {
        return slotBase(baseAddress, slot);
    }


    // These protected methods will be overridden in some subclasses

    protected long key2OfSlot(long slot) {
        return key2At(slotBase(baseAddress, slot));
    }

    protected long key2OfSlot(long baseAddress, long slot) {
        return key2At(slotBase(baseAddress, slot));
    }

    protected long hash(long key1, long key2) {
        return fastLongMix(fastLongMix(key1) + key2);
    }

    protected void putKey(long slot, long key1, long key2) {
        final long slotBase = slotBase(baseAddress, slot);
        AMEM.putLong(slotBase + KEY_1_OFFSET, key1);
        AMEM.putLong(slotBase + KEY_2_OFFSET, key2);
    }


    // These are private instance methods

    private long maskedHash(long key1, long key2) {
        return hash(key1, key2) & mask;
    }

    private boolean isAssigned(long baseAddr, long slot) {
        return AMEM.getLong(slotBase(baseAddr, slot) + offsetOfUnassignedSentinel) != unassignedSentinel;
    }

    private boolean isAssigned(long slot) {
        return isAssigned(baseAddress, slot);
    }

    private long key1OfSlot(long slot) {
        return key1At(slotBase(baseAddress, slot));
    }

    private long valueAddrOfSlot(long slot) {
        return slotBase(baseAddress, slot) + valueOffset;
    }

    private long key1OfSlot(long baseAddress, long slot) {
        return key1At(slotBase(baseAddress, slot));
    }

    private void allocateArrayAndAdjustFields(long newCapacity) {
        baseAddress = malloc.allocate(newCapacity * slotLength);
        capacity = newCapacity;
        mask = newCapacity - 1;
        expandAt = maxSizeForCapacity(newCapacity, loadFactor);
        markAllUnassigned();
    }

    private void markAllUnassigned() {
        AMEM.setMemory(baseAddress, capacity * slotLength, (byte) 0);
        if (unassignedSentinel == 0) {
            return;
        }
        final long addrOfFirstSentinel = baseAddress + offsetOfUnassignedSentinel;
        final int stride = slotLength;
        for (long i = 0; i < capacity; i++) {
            AMEM.putLong(addrOfFirstSentinel + stride * i, unassignedSentinel);
        }
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
                long key1 = key1OfSlot(oldAddress, slot);
                long key2 = key2OfSlot(oldAddress, slot);
                long valueAddress = slotBase(oldAddress, slot) + valueOffset;
                long newSlot = maskedHash(key1, key2);
                while (isAssigned(newSlot)) {
                    newSlot = (newSlot + 1) & mask;
                }
                putKey(newSlot, key1, key2);
                AMEM.copyMemory(valueAddress, valueAddrOfSlot(newSlot), valueLength);
            }
        }
        malloc.free(oldAddress, oldCapacity * slotLength);
    }


    // These public static methods are used by subclasses and also by Hot Restart code

    public static long key1At(long slotBaseAddr) {
        return AMEM.getLong(slotBaseAddr + KEY_1_OFFSET);
    }

    public static long key2At(long slotBaseAddr) {
        return AMEM.getLong(slotBaseAddr + KEY_2_OFFSET);
    }


    private static long maxSizeForCapacity(long capacity, float loadFactor) {
        return Math.max(2, (long) Math.ceil(capacity * loadFactor)) - 1;
    }

    private static long minCapacityForSize(long size, float loadFactor) {
        return roundCapacity((long) Math.ceil(size / loadFactor));
    }

    protected final class Cursor implements HashSlotCursor, HashSlotCursorTwinKey {

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

        @Override public long key() {
            return key1();
        }

        @Override public long key1() {
            ensureValid();
            return key1OfSlot(currentSlot);
        }

        @Override public long key2() {
            ensureValid();
            return key2OfSlot(currentSlot);
        }

        @Override public long valueAddress() {
            ensureValid();
            return valueAddrOfSlot(currentSlot);
        }

        private void ensureValid() {
            ensureLive();
            if (currentSlot < 0) {
                throw new IllegalStateException("Cursor is invalid!");
            }
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
    }
}
