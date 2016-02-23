package com.hazelcast.elastic.binarystorage.sorted;

import com.hazelcast.elastic.binarystorage.BinaryComparator;
import com.hazelcast.elastic.binarystorage.iterator.BinaryKeyIterator;
import com.hazelcast.elastic.binarystorage.iterator.BinaryKeyRedBlackTreeKeysIteratorImpl;
import com.hazelcast.elastic.binarystorage.iterator.value.BinaryValueIterator;
import com.hazelcast.elastic.binarystorage.iterator.value.BinaryValueIteratorImpl;
import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryAccessorType;
import com.hazelcast.memory.MemoryAllocator;

import static com.hazelcast.elastic.binarystorage.sorted.OrderingDirection.ASC;
import static com.hazelcast.elastic.binarystorage.sorted.OrderingDirection.DESC;
import static com.hazelcast.internal.memory.MemoryAccessorProvider.getMemoryAccessor;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;

/**
 * This is classical append-only off-Heap Red Black tree implementation
 * MultiValue for key is available functionality
 */
@SuppressWarnings("checkstyle:methodcount")
public class BinaryKeyValueRedBlackTreeStorage implements BinaryKeyValueSortedStorage {

    // We are using `STANDARD` memory accessor because we internally guarantee that
    // every memory access is aligned.
    protected static final MemoryAccessor MEMORY_ACCESSOR = getMemoryAccessor(MemoryAccessorType.STANDARD);

    // Tree
    protected static final byte RED = 1;

    protected static final byte BLACK = 0;

    protected static final byte LEFT = 1;

    protected static final byte RIGHT = 0;

    // Key
    protected static final int LEFT_LEAF_OFFSET = 0;

    protected static final int RIGHT_LEAF_OFFSET = 8;

    protected static final int KEY_ADDRESS_OFFSET = 16;

    protected static final int KEY_WRITTEN_BYTES_ADDRESS_OFFSET = 24;

    protected static final int KEY_ALLOCATED_BYTES_ADDRESS_OFFSET = 32;

    protected static final int VALUE_ENTRY_OFFSET = 40;

    protected static final int PARENT_ADDRESS_OFFSET = 48;

    protected static final int SIDE_ADDRESS_OFFSET = 56;

    protected static final int COLOR_OFFSET = 57;

    protected static final int MARKER_OFFSET = 58;

    protected static final int KEY_ENTRY_SIZE = MARKER_OFFSET + 1;

    //// Value
    protected static final int VALUE_WRITTEN_BYTES_OFFSET = 0;

    protected static final int VALUE_ALLOCATED_BYTES_OFFSET = 8;

    protected static final int VALUE_ADDRESS_OFFSET = 16;

    protected static final int VALUE_NEXT_VALUE_ENTRY_OFFSET = 24;

    protected static final int LAST_VALUE_ENTRY_OFFSET = 32;

    protected static final int VALUE_ENTRY_SIZE = LAST_VALUE_ENTRY_OFFSET + LONG_SIZE_IN_BYTES;

    protected long rootAddress;

    //// Class
    protected final MemoryAllocator memoryAllocator;

    protected final BinaryComparator binaryKeyComparator;

    protected final BinaryValueIterator binaryValueEntryIterator;

    protected final BinaryKeyIterator binaryKeyIterator;

    public BinaryKeyValueRedBlackTreeStorage(MemoryAllocator memoryAllocator, BinaryComparator binaryKeyComparator) {
        this.memoryAllocator = memoryAllocator;
        this.binaryKeyComparator = binaryKeyComparator;
        this.binaryValueEntryIterator = new BinaryValueIteratorImpl(this);
        this.binaryKeyIterator = new BinaryKeyRedBlackTreeKeysIteratorImpl(this);
    }

    // Key Public methods
    @Override
    public long getValueEntryAddress(long keyEntryAddress) {
        return MEMORY_ACCESSOR.getLong(getValueEntryAddressOffset(keyEntryAddress));
    }

    // Value public methods
    @Override
    public long getNextValueEntryAddress(long valueEntryAddress) {
        long offset = getNextValueEntryOffset(valueEntryAddress);

        if (offset == 0) {
            return 0;
        } else {
            return MEMORY_ACCESSOR.getLong(offset);
        }
    }

    @Override
    public long getValueAddress(long valueEntryAddress) {
        long offset = getValueAddressOffset(valueEntryAddress);
        return MEMORY_ACCESSOR.getLong(offset);
    }

    @Override
    public long getValueWrittenBytes(long valueEntryAddress) {
        long offset = getValueWrittenBytesOffset(valueEntryAddress);
        return MEMORY_ACCESSOR.getLong(offset);
    }

    @Override
    public long getValueAllocatedBytes(long valueEntryAddress) {
        long offset = getValueAllocatedBytesOffset(valueEntryAddress);
        return MEMORY_ACCESSOR.getLong(offset);
    }

    @Override
    public BinaryKeyIterator keyIterator() {
        return keyIterator(ASC);
    }

    protected long getNextValueEntryOffset(long valueEntryAddress) {
        return valueEntryAddress + VALUE_NEXT_VALUE_ENTRY_OFFSET;
    }

    protected long getLastValueEntryAddressOffset(long valueEntryAddress) {
        return valueEntryAddress + LAST_VALUE_ENTRY_OFFSET;
    }

    protected long acquireNewKeyEntry(byte color) {
        long address = memoryAllocator.allocate(KEY_ENTRY_SIZE);
        MEMORY_ACCESSOR.setMemory(address, KEY_ENTRY_SIZE, (byte) 0);
        MEMORY_ACCESSOR.putByte(getColorAddressOffset(address), color);
        return address;
    }

    protected long allocateNewValueEntry() {
        long address = memoryAllocator.allocate(VALUE_ENTRY_SIZE);
        MEMORY_ACCESSOR.setMemory(address, VALUE_ENTRY_SIZE, (byte) 0);
        return address;
    }

    protected long getKeyWrittenBytesAddressOffset(long keyEntryAddress) {
        return keyEntryAddress + KEY_WRITTEN_BYTES_ADDRESS_OFFSET;
    }

    protected long getKeyAllocatedBytesAddressOffset(long keyEntryAddress) {
        return keyEntryAddress + KEY_ALLOCATED_BYTES_ADDRESS_OFFSET;
    }

    protected long getValueEntryAddressOffset(long keyEntryAddress) {
        return keyEntryAddress + VALUE_ENTRY_OFFSET;
    }

    protected long getLeftAddressOffset(long entryAddress) {
        return entryAddress + LEFT_LEAF_OFFSET;
    }

    protected long getRightAddressOffset(long entryAddress) {
        return entryAddress + RIGHT_LEAF_OFFSET;
    }

    protected long getKeyAddressOffset(long entryAddress) {
        return entryAddress + KEY_ADDRESS_OFFSET;
    }

    protected long getParentAddressOffset(long keyEntryAddress) {
        return keyEntryAddress + PARENT_ADDRESS_OFFSET;
    }

    protected long getSideAddressOffset(long keyEntryAddress) {
        return keyEntryAddress + SIDE_ADDRESS_OFFSET;
    }

    protected long getColorAddressOffset(long keyEntryAddress) {
        return keyEntryAddress + COLOR_OFFSET;
    }

    protected long getMerkerAddressOffset(long keyEntryAddress) {
        return keyEntryAddress + MARKER_OFFSET;
    }

    protected long getValueWrittenBytesOffset(long keyEntryAddress) {
        return keyEntryAddress + VALUE_WRITTEN_BYTES_OFFSET;
    }

    protected long getValueAllocatedBytesOffset(long keyEntryAddress) {
        return keyEntryAddress + VALUE_ALLOCATED_BYTES_OFFSET;
    }

    protected long getValueAddressOffset(long valueEntryAddress) {
        return valueEntryAddress + VALUE_ADDRESS_OFFSET;
    }

    /**
     * @param direction ASC - 1
     *                  DESC - 0
     * @return iterator over keys for the specified direction
     */
    @Override
    public BinaryKeyIterator keyIterator(OrderingDirection direction) {
        binaryKeyIterator.setDirection(direction);
        return binaryKeyIterator;
    }

    /**
     * @param keyEntryPointer - address of the key entry
     * @return iterator over values for the specified direction
     */
    @Override
    public BinaryValueIterator valueIterator(long keyEntryPointer) {
        binaryValueEntryIterator.reset(keyEntryPointer);
        return binaryValueEntryIterator;
    }

    private int compareKeys(BinaryComparator comparator, long keyAddress, long keySize, long entryAddress) {
        return comparator.compare(
                keyAddress,
                keySize,
                getKeyAddress(entryAddress),
                getKeyWrittenBytes(entryAddress)
        );
    }

    private void setKey(long address, long keyAddress, long keyLength, long keyAllocatedBytes) {
        MEMORY_ACCESSOR.putLong(getKeyAddressOffset(address), keyAddress);
        MEMORY_ACCESSOR.putLong(getKeyWrittenBytesAddressOffset(address), keyLength);
        MEMORY_ACCESSOR.putLong(getKeyAllocatedBytesAddressOffset(address), keyAllocatedBytes);
    }

    /**
     * Looking for key equal to buffer by address;
     * keyAddress with params keyWrittenBytes and  keyAllocatedBytes;
     * In case if found - append value represented by;
     * valueAddress with params  valueWrittenBytes and  valueAllocatedBytes to the chain of key's values;
     * <p/>
     * New key is inserted in accordance with Comparator;
     *
     * @param keyAddress          - address of the key;
     * @param keyWrittenBytes     - amount of written bytes of the key;
     * @param keyAllocatedBytes   - amount of allocated bytes of the key;
     * @param valueAddress        - address of the value;
     * @param valueWrittenBytes   - amount of written bytes of the value;
     * @param valueAllocatedBytes - amount of allocated bytes of the value;
     * @return address of the key entry;
     */
    @Override
    public long put(long keyAddress, long keyWrittenBytes, long keyAllocatedBytes,
                    long valueAddress, long valueWrittenBytes, long valueAllocatedBytes) {
        return put(keyAddress, keyWrittenBytes, keyAllocatedBytes,
                valueAddress, valueWrittenBytes, valueAllocatedBytes,
                binaryKeyComparator
        );
    }

    /**
     * Looking for key equal to buffer by address;
     * keyAddress with params keyWrittenBytes and  keyAllocatedBytes;
     * In case if found - append value represented by;
     * valueAddress with params  valueWrittenBytes and  valueAllocatedBytes to the chain of key's values;
     * <p/>
     * New key is inserted in accordance with Comparator;
     *
     * @param keyAddress          - address of the key;
     * @param keyWrittenBytes     - amount of written bytes of the key;
     * @param keyAllocatedBytes   - amount of allocated bytes of the key;
     * @param valueAddress        - address of the value;
     * @param valueWrittenBytes   - amount of written bytes of the value;
     * @param valueAllocatedBytes - amount of allocated bytes of the value;
     * @param comparator          - comparator to be used, if null default comparator passed from constructor to be used;
     * @return address of the key entry;
     */
    @Override
    public long put(long keyAddress, long keyWrittenBytes, long keyAllocatedBytes,
                    long valueAddress, long valueWrittenBytes, long valueAllocatedBytes, BinaryComparator comparator) {
        long keyEntryAddress = getKeyEntry(keyAddress, keyWrittenBytes, keyAllocatedBytes, comparator, true);

        insertValue(
                valueAddress,
                valueWrittenBytes,
                valueAllocatedBytes,
                keyEntryAddress
        );

        return keyEntryAddress;
    }

    protected long insertValue(long valueAddress,
                               long valueWrittenBytes,
                               long valueAllocatedBytes,
                               long keyEntryAddress) {
        long valueEntryOffsetAddress = getValueEntryAddressOffset(keyEntryAddress);
        long newValueEntryAddress = allocateNewValueEntry();

        MEMORY_ACCESSOR.putLong(getValueAddressOffset(newValueEntryAddress), valueAddress);
        MEMORY_ACCESSOR.putLong(getValueWrittenBytesOffset(newValueEntryAddress), valueWrittenBytes);
        MEMORY_ACCESSOR.putLong(getValueAllocatedBytesOffset(newValueEntryAddress), valueAllocatedBytes);

        long valueEntryAddress = MEMORY_ACCESSOR.getLong(valueEntryOffsetAddress);

        if (valueEntryAddress == 0L) {
            // It is the first element - setting link to it
            MEMORY_ACCESSOR.putLong(valueEntryOffsetAddress, newValueEntryAddress);
            // Set last address
            MEMORY_ACCESSOR.putLong(getLastValueEntryAddressOffset(newValueEntryAddress), newValueEntryAddress);
        } else {
            long lastValueEntryAddress = MEMORY_ACCESSOR.getLong(getLastValueEntryAddressOffset(valueEntryAddress));
            if (lastValueEntryAddress > 0L) {
                // Link old last value with new
                MEMORY_ACCESSOR.putLong(getNextValueEntryOffset(lastValueEntryAddress), newValueEntryAddress);
            }

            // Set last address to key
            MEMORY_ACCESSOR.putLong(getLastValueEntryAddressOffset(valueEntryAddress), newValueEntryAddress);
        }

        return newValueEntryAddress;
    }

    /**
     * Looking for key equal to buffer by address;
     * keyAddress with params keyWrittenBytes and keyAllocatedBytes;
     * <p/>
     * In case if not-found - if createIfNotExists==true - create new key entry;
     * if createIfNotExists==false - don't create new key entry - return 0L;
     *
     * @param keyAddress        - address of the key
     * @param keyWrittenBytes   - amount of written bytes of the key
     * @param keyAllocatedBytes - amount of allocated bytes of the key
     * @param createIfNotExists - flag which determines if we want to create entry if it doesn't exist
     * @return address of the key entry
     */
    @Override
    public long getKeyEntry(long keyAddress, long keyWrittenBytes, long keyAllocatedBytes, boolean createIfNotExists) {
        return getKeyEntry(keyAddress, keyWrittenBytes, keyAllocatedBytes,
                binaryKeyComparator, createIfNotExists);
    }

    /**
     * Looking for key equal to buffer by address;
     * keyAddress with params keyWrittenBytes and keyAllocatedBytes;
     * <p/>
     * In case if not-found - if createIfNotExists==true - create new key entry;
     * if createIfNotExists==false - don't create new key entry - return 0L;
     *
     * @param keyAddress        - address of the key;
     * @param keyWrittenBytes   - amount of written bytes of the key;
     * @param keyAllocatedBytes - amount of allocated bytes of the key;
     * @param comparator        - comparator to be used, if null default passed in constructor will be used;
     * @param createIfNotExists - flag which determines if we want to create entry if it doesn't exist;
     * @return address of the key entry
     */
    @Override
    public long getKeyEntry(long keyAddress, long keyWrittenBytes, long keyAllocatedBytes,
                            BinaryComparator comparator, boolean createIfNotExists) {
        if (rootAddress == 0L) {
            if (createIfNotExists) {
                rootAddress = memoryAllocator.allocate(LONG_SIZE_IN_BYTES);
            } else {
                return 0L;
            }
        }
        return getKeyEntry0(
                rootAddress,
                comparator == null ? binaryKeyComparator : comparator,
                keyAddress,
                keyWrittenBytes,
                keyAllocatedBytes,
                createIfNotExists
        );
    }

    private long getRootKeyEntry(long rootAddress) {
        return MEMORY_ACCESSOR.getLong(rootAddress);
    }

    private void setRootKeyEntry(long rootAddress, long keyEntryAddress) {
        MEMORY_ACCESSOR.putLong(rootAddress, keyEntryAddress);
    }

    protected long getKeyEntry0(long rootAddress,
                                BinaryComparator comparator,
                                long keyAddress,
                                long keyWrittenBytes,
                                long keyAllocatedBytes,
                                boolean createIfNotExists) {
        if (rootAddress == 0L) {
            return 0L;
        }

        long rootKeyEntry = getRootKeyEntry(rootAddress);

        if (rootKeyEntry == 0L) {
            if (createIfNotExists) {
                rootKeyEntry = acquireNewKeyEntry(BLACK);
                setRootKeyEntry(rootAddress, rootKeyEntry);
            } else {
                return 0L;
            }

            setKey(rootKeyEntry, keyAddress, keyWrittenBytes, keyAllocatedBytes);
            return rootKeyEntry;
        }

        long address = rootKeyEntry;

        while (true) {
            int compareResult = compareKeys(comparator, keyAddress, keyWrittenBytes, address);

            if (compareResult == 1) {
                // Our key is greater
                long rightAddress = getRightAddressOffset(address);
                if (MEMORY_ACCESSOR.getLong(rightAddress) == 0) {
                    if (createIfNotExists) {
                        return assignNewLeaf(rootAddress, keyAddress, keyWrittenBytes, keyAllocatedBytes,
                                address, rightAddress, RED, RIGHT);
                    } else {
                        return 0L;
                    }
                } else {
                    address = MEMORY_ACCESSOR.getLong(rightAddress);
                }
            } else if (compareResult == -1) {
                // Our key is less
                long leftAddress = getLeftAddressOffset(address);

                if (MEMORY_ACCESSOR.getLong(leftAddress) == 0) {
                    if (createIfNotExists) {
                        return assignNewLeaf(rootAddress, keyAddress, keyWrittenBytes, keyAllocatedBytes,
                                address, leftAddress, RED, LEFT);
                    } else {
                        return 0L;
                    }
                } else {
                    address = MEMORY_ACCESSOR.getLong(leftAddress);
                }
            } else {
                // Keys are same
                return address;
            }
        }
    }

    private long assignNewLeaf(
            long rootAddress,
            long keyAddress,
            long keyWrittenBytes,
            long keyAllocatedBytes,
            long entryAddress,
            long leafAddress,
            byte color,
            byte side) {
        long address = acquireNewKeyEntry(color);
        setKey(address, keyAddress, keyWrittenBytes, keyAllocatedBytes);

        MEMORY_ACCESSOR.putLong(leafAddress, address);

        long parentAddress = getParentAddressOffset(address);
        MEMORY_ACCESSOR.putLong(parentAddress, entryAddress);

        long sideOffset = getSideAddressOffset(address);
        MEMORY_ACCESSOR.putByte(sideOffset, side);

        checkRedBlackConsistency(rootAddress, entryAddress, address, side);

        return address;
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:methodlength" })
    private void checkRedBlackConsistency(long rootAddress, long fatherAddress, long sonAddress, byte sonSide) {
        if (fatherAddress == 0L) {
            blackRoot(rootAddress);
            return;
        }

        long grandFatherAddress;
        long grandFatherOffsetAddress = getParentAddressOffset(fatherAddress);

        if (grandFatherOffsetAddress > 0L) {
            grandFatherAddress = MEMORY_ACCESSOR.getLong(grandFatherOffsetAddress);
        } else {
            return;
        }

        if (MEMORY_ACCESSOR.getByte(getColorAddressOffset(fatherAddress)) == BLACK) {
            return;
        }

        byte fathersSide = MEMORY_ACCESSOR.getByte(getSideAddressOffset(fatherAddress));
        long uncleAddress;

        if (fathersSide == LEFT) {
            long rightAddress = getRightAddressOffset(grandFatherAddress);
            uncleAddress = rightAddress > 0L ? MEMORY_ACCESSOR.getLong(rightAddress) : 0L;
        } else {
            long leftAddress = getLeftAddressOffset(grandFatherAddress);
            uncleAddress = leftAddress > 0L ? MEMORY_ACCESSOR.getLong(leftAddress) : 0L;
        }

        // Case 1 - red uncle
        if (uncleAddress > 0L) {
            if (case1(rootAddress, fatherAddress, grandFatherAddress, uncleAddress)) {
                return;
            }
        }

        // Case 2: Son's and father's side are different, uncle is black or absent
        if ((sonSide != fathersSide)) {
            case2(fatherAddress, sonAddress, sonSide, grandFatherAddress, fathersSide);

            // Switch father and son address
            long tmp = fatherAddress;
            fatherAddress = sonAddress;
            sonAddress = tmp;

            sonSide = MEMORY_ACCESSOR.getByte(getSideAddressOffset(sonAddress));
            fathersSide = MEMORY_ACCESSOR.getByte(getSideAddressOffset(fatherAddress));
        }

        // Case 3: Son's and father's side are the same, uncle is black or absent
        if (sonSide == fathersSide) {
            if (case3(rootAddress, fatherAddress, sonSide, grandFatherAddress, fathersSide)) {
                return;
            }
        }
        blackRoot(rootAddress);
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private boolean case3(long rootAddress, long fatherAddress, byte sonSide, long grandFatherAddress, byte fathersSide) {
        if (grandFatherAddress == 0) {
            return true;
        }

        long parentOffsetAddress = getParentAddressOffset(grandFatherAddress);
        long grandFathersParentEntry = parentOffsetAddress > 0L ? MEMORY_ACCESSOR.getLong(parentOffsetAddress) : 0L;

        // Grandfather's migration
        if (grandFathersParentEntry == 0L) {
            MEMORY_ACCESSOR.putLong(rootAddress, fatherAddress);

            MEMORY_ACCESSOR.putLong(getParentAddressOffset(fatherAddress), 0L);
        } else {
            byte grandFathersSide = MEMORY_ACCESSOR.getByte(getSideAddressOffset(grandFatherAddress));

            if (grandFathersSide == LEFT) {
                MEMORY_ACCESSOR.putLong(getLeftAddressOffset(grandFathersParentEntry), fatherAddress);
                // Set father's side
                MEMORY_ACCESSOR.putByte(getSideAddressOffset(fatherAddress), LEFT);
                // Father on grandFather's father
                MEMORY_ACCESSOR.putLong(getParentAddressOffset(fatherAddress), grandFathersParentEntry);
            } else {
                MEMORY_ACCESSOR.putLong(getRightAddressOffset(grandFathersParentEntry), fatherAddress);
                // Set father's side
                MEMORY_ACCESSOR.putByte(getSideAddressOffset(fatherAddress), RIGHT);
                // Father on grandFather's father
                MEMORY_ACCESSOR.putLong(getParentAddressOffset(fatherAddress), grandFathersParentEntry);
            }
        }

        long grandFathersLeftOffset = getLeftAddressOffset(grandFatherAddress);
        long grandFathersRightOffset = getRightAddressOffset(grandFatherAddress);

        // Father's migration
        long fathersLeftOffset = getLeftAddressOffset(fatherAddress);
        long fathersRightOffset = getRightAddressOffset(fatherAddress);

        long fatherLeftAddress = MEMORY_ACCESSOR.getLong(fathersLeftOffset);
        long fatherRightAddress = MEMORY_ACCESSOR.getLong(fathersRightOffset);

        // Father link to grandfather
        // Grandfather links to father's another child
        if (sonSide == LEFT) {
            MEMORY_ACCESSOR.putLong(fathersRightOffset, grandFatherAddress);
            MEMORY_ACCESSOR.putLong(grandFathersLeftOffset, fatherRightAddress);
            // GrandFather on father
            MEMORY_ACCESSOR.putLong(getParentAddressOffset(grandFatherAddress), fatherAddress);

            if (fatherRightAddress > 0L) {
                // Set father's child to grandFather
                MEMORY_ACCESSOR.putByte(getSideAddressOffset(fatherRightAddress), LEFT);
                MEMORY_ACCESSOR.putLong(getParentAddressOffset(fatherRightAddress), grandFatherAddress);
            }
            // Set grandFather's side
            MEMORY_ACCESSOR.putByte(getSideAddressOffset(grandFatherAddress), RIGHT);
        } else {
            MEMORY_ACCESSOR.putLong(fathersLeftOffset, grandFatherAddress);
            MEMORY_ACCESSOR.putLong(grandFathersRightOffset, fatherLeftAddress);
            // GrandFather on father
            MEMORY_ACCESSOR.putLong(getParentAddressOffset(grandFatherAddress), fatherAddress);

            // Set father's child to grandFather
            if (fatherLeftAddress > 0L) {
                MEMORY_ACCESSOR.putByte(getSideAddressOffset(fatherLeftAddress), RIGHT);
                MEMORY_ACCESSOR.putLong(getParentAddressOffset(fatherLeftAddress), grandFatherAddress);
            }
            // Set grandFather's side
            MEMORY_ACCESSOR.putByte(getSideAddressOffset(grandFatherAddress), LEFT);
        }

        // Changing color
        // GrandFather - red
        MEMORY_ACCESSOR.putByte(getColorAddressOffset(grandFatherAddress), RED);
        // Father - black
        MEMORY_ACCESSOR.putByte(getColorAddressOffset(fatherAddress), BLACK);
        // Grandfather's side will become uncle's side
        MEMORY_ACCESSOR.putByte(getSideAddressOffset(grandFatherAddress), fathersSide == LEFT ? RIGHT : LEFT);
        return false;
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private void case2(long fatherAddress, long sonAddress, byte sonSide, long grandFatherAddress, byte fathersSide) {
        long grandFathersOffset = fathersSide == LEFT
                ? getLeftAddressOffset(grandFatherAddress)
                : getRightAddressOffset(grandFatherAddress);
        MEMORY_ACCESSOR.putLong(grandFathersOffset, sonAddress);

        long sonLeftAddress = getLeftAddressOffset(sonAddress);
        long sonRightAddress = getRightAddressOffset(sonAddress);
        long sonsChild = sonSide == RIGHT ? MEMORY_ACCESSOR.getLong(sonLeftAddress) : MEMORY_ACCESSOR.getLong(sonRightAddress);

        // Son's left becomes father's right
        MEMORY_ACCESSOR.putLong(
                sonSide == RIGHT ? getRightAddressOffset(fatherAddress) : getLeftAddressOffset(fatherAddress),
                sonsChild);

        if (sonsChild > 0L) {
            MEMORY_ACCESSOR.putLong(getParentAddressOffset(sonsChild), fatherAddress);
            MEMORY_ACCESSOR.putByte(getSideAddressOffset(sonsChild), sonSide);
        }

        // Son becomes parent of father
        MEMORY_ACCESSOR.putLong(sonSide == RIGHT ? sonLeftAddress : sonRightAddress, fatherAddress);

        // Set new parents to father and son
        MEMORY_ACCESSOR.putLong(getParentAddressOffset(fatherAddress), sonAddress);
        MEMORY_ACCESSOR.putLong(getParentAddressOffset(sonAddress), grandFatherAddress);

        // Change son's side
        MEMORY_ACCESSOR.putByte(getSideAddressOffset(sonAddress), sonSide == LEFT ? RIGHT : LEFT);
    }

    private boolean case1(long rootAddress, long fatherAddress, long grandFatherAddress, long uncleAddress) {
        byte uncleColor = MEMORY_ACCESSOR.getByte(getColorAddressOffset(uncleAddress));

        if (uncleColor == RED) {
            // Repaint uncle to black
            MEMORY_ACCESSOR.putByte(getColorAddressOffset(uncleAddress), BLACK);
            // Repaint father to black
            MEMORY_ACCESSOR.putByte(getColorAddressOffset(fatherAddress), BLACK);

            if (grandFatherAddress > 0L) {
                // Repaint grand-father to red
                MEMORY_ACCESSOR.putByte(getColorAddressOffset(grandFatherAddress), RED);

                byte grandFatherSide = MEMORY_ACCESSOR.getByte(getSideAddressOffset(grandFatherAddress));
                long grandFatherParentOffsetAddress = getParentAddressOffset(grandFatherAddress);
                long grandFatherParentAddress = grandFatherParentOffsetAddress > 0L
                        ? MEMORY_ACCESSOR.getLong(grandFatherParentOffsetAddress)
                        : 0L;

                // Recursive call
                checkRedBlackConsistency(rootAddress, grandFatherParentAddress, grandFatherAddress, grandFatherSide);
            } else {
                long rootKeyEntry = getRootKeyEntry(rootAddress);
                if (rootKeyEntry > 0L) {
                    MEMORY_ACCESSOR.putByte(getColorAddressOffset(rootKeyEntry), BLACK);
                }
            }

            return true;
        }
        return false;
    }

    private void blackRoot(long rootAddress) {
        long rootEntry = getRootKeyEntry(rootAddress);

        if (rootEntry > 0L) {
            MEMORY_ACCESSOR.putByte(getColorAddressOffset(rootEntry), BLACK);
        }
    }

    @Override
    public long first(OrderingDirection direction) {
        if (rootAddress == 0) {
            return 0L;
        }

        return first(direction, getRootKeyEntry(rootAddress));
    }

    @Override
    public long first(OrderingDirection direction, long keyEntry) {
        if (keyEntry == 0L) {
            return 0L;
        }

        long pointer = keyEntry;

        while (true) {
            long next;

            if (direction == ASC) {
                next = MEMORY_ACCESSOR.getLong(getLeftAddressOffset(pointer));
            } else {
                next = MEMORY_ACCESSOR.getLong(getRightAddressOffset(pointer));
            }

            if (next == 0L) {
                return pointer;
            }

            pointer = next;
        }
    }

    private boolean checkSide(byte side, OrderingDirection direction) {
        return (((side == LEFT) && (direction == ASC)) || ((side == RIGHT) && (direction == DESC)));
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public long getNext(long pointer, OrderingDirection direction) {
        long childAddress = direction == ASC ? getRightAddressOffset(pointer) : getLeftAddressOffset(pointer);

        if (childAddress > 0L) {
            long child = MEMORY_ACCESSOR.getLong(childAddress);

            if (child > 0L) {
                return first(direction, child);
            }
        }

        byte side = MEMORY_ACCESSOR.getByte(
                getSideAddressOffset(pointer));

        if (checkSide(side, direction)) {
            long parentAddress = MEMORY_ACCESSOR.getLong(getParentAddressOffset(pointer));

            if (parentAddress > 0L) {
                return parentAddress;
            }
        }

        while (!checkSide(side, direction)) {
            long parentAddress = MEMORY_ACCESSOR.getLong(getParentAddressOffset(pointer));

            if (parentAddress == 0) {
                return 0;
            } else {
                pointer = parentAddress;
                side = MEMORY_ACCESSOR.getByte(getSideAddressOffset(pointer));
            }
        }

        return MEMORY_ACCESSOR.getLong(getParentAddressOffset(pointer));
    }

    @Override
    public long getKeyAddress(long keyEntryPointer) {
        if (keyEntryPointer == 0L) {
            return 0L;
        }

        if (getKeyAddressOffset(keyEntryPointer) > 0L) {
            return MEMORY_ACCESSOR.getLong(getKeyAddressOffset(keyEntryPointer));
        } else {
            return 0;
        }
    }

    @Override
    public long getKeyWrittenBytes(long keyEntryPointer) {
        if (keyEntryPointer == 0L) {
            return 0L;
        }

        if (getKeyWrittenBytesAddressOffset(keyEntryPointer) > 0L) {
            return MEMORY_ACCESSOR.getLong(getKeyWrittenBytesAddressOffset(keyEntryPointer));
        } else {
            return 0;
        }
    }

    @Override
    public long getKeyAllocatedBytes(long keyEntryPointer) {
        if (keyEntryPointer == 0L) {
            return 0L;
        }

        if (getKeyAllocatedBytesAddressOffset(keyEntryPointer) > 0L) {
            return MEMORY_ACCESSOR.getLong(getKeyAllocatedBytesAddressOffset(keyEntryPointer));
        } else {
            return 0L;
        }
    }

    @Override
    public void markKeyEntry(long keyEntryPointer, byte marker) {
        if (keyEntryPointer != 0L) {
            MEMORY_ACCESSOR.putByte(getMerkerAddressOffset(keyEntryPointer), marker);
        }
    }

    @Override
    public byte getKeyEntryMarker(long keyEntryPointer) {
        if (keyEntryPointer != 0L) {
            return MEMORY_ACCESSOR.getByte(getMerkerAddressOffset(keyEntryPointer));
        } else {
            return 0;
        }
    }

    @Override
    public long count() {
        if (rootAddress > 0L) {
            long rootKeyEntry = getRootKeyEntry(rootAddress);
            return rootKeyEntry == 0 ? 0 : count0(rootKeyEntry);
        }

        return 0;
    }

    @Override
    public boolean validate() {
        if (rootAddress > 0L) {
            long rootKeyEntry = getRootKeyEntry(rootAddress);
            return validate0(rootKeyEntry, 0L);
        } else {
            return true;
        }
    }

    private boolean validate0(long pointer, long father) {
        long parentAddressOffset = getParentAddressOffset(pointer);
        long sideOffset = getSideAddressOffset(pointer);
        long parentAddress = MEMORY_ACCESSOR.getLong(parentAddressOffset);

        if (father != parentAddress) {
            return false;
        }

        if (parentAddress > 0L) {
            byte side = MEMORY_ACCESSOR.getByte(sideOffset);
            long childAddress = MEMORY_ACCESSOR.getLong(
                    side == LEFT ? getLeftAddressOffset(parentAddress) : getRightAddressOffset(parentAddress));

            if (childAddress != pointer) {
                return false;
            }
        } else {
            if (pointer != getRootKeyEntry(rootAddress)) {
                return false;
            }
        }

        return !(validateChild(pointer, LEFT) || validateChild(pointer, RIGHT));

    }

    private boolean validateChild(long pointer, byte side) {
        long childAddress = MEMORY_ACCESSOR.getLong(
                side == LEFT ? getLeftAddressOffset(pointer) : (getRightAddressOffset(pointer))
        );

        if (childAddress > 0L) {
            boolean result = validate0(childAddress, pointer);

            if (!result) {
                return true;
            }
        }

        return false;
    }

    private long count0(long pointer) {
        long counter = 1;

        if (MEMORY_ACCESSOR.getLong(getLeftAddressOffset(pointer)) > 0L) {
            counter += count0(MEMORY_ACCESSOR.getLong(getLeftAddressOffset(pointer)));
        }

        if (MEMORY_ACCESSOR.getLong(getRightAddressOffset(pointer)) > 0L) {
            counter += count0(MEMORY_ACCESSOR.getLong(getRightAddressOffset(pointer)));
        }

        return counter;
    }

    @Override
    public void dispose() {
        if (rootAddress > 0L) {
            dispose0(getRootKeyEntry(rootAddress));
            freeRoot();
        }
    }

    private void dispose0(long rootKeyEntryAddress) {
        if (rootKeyEntryAddress != 0L) {
            release0(rootKeyEntryAddress, true);
        }
    }

    @Override
    public void disposeEntries() {
        if (rootAddress > 0L) {
            long rootKeyEntry = getRootKeyEntry(rootAddress);
            release0(rootKeyEntry, false);
            freeRoot();
        }
    }

    protected void freeRoot() {
        memoryAllocator.free(rootAddress, LONG_SIZE_IN_BYTES);
        rootAddress = 0L;
    }

    protected void release0(long keyEntryAddress, boolean releasePayLoad) {
        if (MEMORY_ACCESSOR.getLong(getLeftAddressOffset(keyEntryAddress)) > 0L) {
            release0(MEMORY_ACCESSOR.getLong(getLeftAddressOffset(keyEntryAddress)), releasePayLoad);
        }

        if (MEMORY_ACCESSOR.getLong(getRightAddressOffset(keyEntryAddress)) > 0L) {
            release0(MEMORY_ACCESSOR.getLong(getRightAddressOffset(keyEntryAddress)), releasePayLoad);
        }

        releaseKeyEntry(keyEntryAddress, releasePayLoad);
    }

    protected void releaseKeyEntry(long keyEntryAddress, boolean releasePayLoad) {
        releaseKeyEntry(keyEntryAddress, releasePayLoad, true);
    }

    protected void releaseKeyEntry(long keyEntryAddress, boolean releasePayLoad, boolean releaseValue) {
        if (keyEntryAddress > 0L) {
            // Release Key PayLoad
            long keyAddress = getKeyAddress(keyEntryAddress);

            if ((keyAddress > 0L) && (releasePayLoad)) {
                memoryAllocator.free(keyAddress, getKeyAllocatedBytes(keyEntryAddress));
            }

            if (releaseValue) {
                // Release Values
                releaseValue(keyEntryAddress, releasePayLoad);
            }

            // Release Key Entry
            memoryAllocator.free(keyEntryAddress, KEY_ENTRY_SIZE);
        }
    }

    protected void releaseValue(long keyEntryAddress, boolean releasePayLoad) {
        long valueEntryAddressOffset = getValueEntryAddressOffset(keyEntryAddress);
        long valueEntryAddress = MEMORY_ACCESSOR.getLong(valueEntryAddressOffset);

        while (valueEntryAddress > 0L) {
            // Release Value PayLoad
            if (releasePayLoad) {
                releaseValuePayLoad(valueEntryAddress);
            }

            long oldValueEntryAddress = valueEntryAddress;

            valueEntryAddress = MEMORY_ACCESSOR.getLong(getNextValueEntryOffset(valueEntryAddress));

            if (oldValueEntryAddress > 0L) {
                // Release Value Entry
                memoryAllocator.free(oldValueEntryAddress, VALUE_ENTRY_SIZE);
            }
        }
    }

    protected void releaseValuePayLoad(long valueEntryAddress) {
        long valueAddressOffset = getValueAddressOffset(valueEntryAddress);

        if (valueAddressOffset > 0L) {
            long valueAddress = MEMORY_ACCESSOR.getLong(valueAddressOffset);

            if (valueAddress > 0L) {
                long allocatedBytes = 0;
                if (getValueAllocatedBytesOffset(valueEntryAddress) > 0L) {
                    allocatedBytes = MEMORY_ACCESSOR.getLong(getValueAllocatedBytesOffset(valueEntryAddress));
                }

                memoryAllocator.free(valueAddress, allocatedBytes);
            }
        }
    }
}
