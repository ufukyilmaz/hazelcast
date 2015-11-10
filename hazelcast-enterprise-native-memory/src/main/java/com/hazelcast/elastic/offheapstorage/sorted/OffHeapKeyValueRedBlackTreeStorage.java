package com.hazelcast.elastic.offheapstorage.sorted;


import sun.misc.Unsafe;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.elastic.offheapstorage.OffHeapComparator;
import com.hazelcast.elastic.offheapstorage.iterator.OffHeapKeyIterator;
import com.hazelcast.elastic.offheapstorage.iterator.value.OffHeapValueIterator;
import com.hazelcast.elastic.offheapstorage.iterator.value.OffHeapValueIteratorImpl;
import com.hazelcast.elastic.offheapstorage.iterator.OffHeapKeyRedBlackTreeKeysIteratorImpl;

import static com.hazelcast.elastic.offheapstorage.sorted.OrderingDirection.ASC;
import static com.hazelcast.elastic.offheapstorage.sorted.OrderingDirection.DESC;


/***
 * This is classical append-only off-Heap Red Black tree implementation
 * MultiValue for key is available functionality
 */
public class OffHeapKeyValueRedBlackTreeStorage implements OffHeapKeyValueSortedStorage {
    protected static final Unsafe UNSAFE = UnsafeHelper.UNSAFE;

    protected long rootAddress;

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

    protected static final int VALUE_ENTRY_SIZE = LAST_VALUE_ENTRY_OFFSET + 8;

    //// Class
    protected final MemoryAllocator memoryAllocator;

    protected final OffHeapComparator offHeapKeyComparator;

    protected final OffHeapValueIterator offHeapValueEntryIterator;

    protected final OffHeapKeyIterator offHeapKeyIterator;

    public OffHeapKeyValueRedBlackTreeStorage(
            MemoryAllocator memoryAllocator,
            OffHeapComparator offHeapKeyComparator
    ) {
        this.memoryAllocator = memoryAllocator;
        this.offHeapKeyComparator = offHeapKeyComparator;
        this.offHeapValueEntryIterator = new OffHeapValueIteratorImpl(this);
        this.offHeapKeyIterator = new OffHeapKeyRedBlackTreeKeysIteratorImpl(this);
    }

    //Key Public methods
    @Override
    public long getValueEntryAddress(long keyEntryAddress) {
        return UNSAFE.getLong(getValueEntryAddressOffset(keyEntryAddress));
    }

    // Value public methods
    @Override
    public long getNextValueEntryAddress(long valueEntryAddress) {
        long offset = getNextValueEntryOffset(valueEntryAddress);

        if (offset == 0) {
            return 0;
        } else {
            return UNSAFE.getLong(offset);
        }
    }

    @Override
    public long getValueAddress(long valueEntryAddress) {
        long offset = getValueAddressOffset(valueEntryAddress);
        return UNSAFE.getLong(offset);
    }

    @Override
    public long getValueWrittenBytes(long valueEntryAddress) {
        long offset = getValueWrittenBytesOffset(valueEntryAddress);
        return UNSAFE.getLong(offset);
    }

    @Override
    public long getValueAllocatedBytes(long valueEntryAddress) {
        long offset = getValueAllocatedBytesOffset(valueEntryAddress);
        return UNSAFE.getLong(offset);
    }

    @Override
    public OffHeapKeyIterator keyIterator() {
        return keyIterator(ASC);
    }

    protected long getNextValueEntryOffset(long valueEntryAddress) {
        return valueEntryAddress + VALUE_NEXT_VALUE_ENTRY_OFFSET;
    }

    protected long getLastValueEntryAddressOffset(long valueEntryAddress) {
        return valueEntryAddress + LAST_VALUE_ENTRY_OFFSET;
    }

    protected long acquireNewKeyEntry(byte color) {
        long address = this.memoryAllocator.allocate(KEY_ENTRY_SIZE);
        UNSAFE.setMemory(address, KEY_ENTRY_SIZE, (byte) 0);
        UNSAFE.putByte(getColorAddressOffset(address), color);
        return address;
    }

    protected long allocateNewValueEntry() {
        long address = this.memoryAllocator.allocate(VALUE_ENTRY_SIZE);
        UNSAFE.setMemory(address, VALUE_ENTRY_SIZE, (byte) 0);
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


    /***
     * @param direction ASC - 1
     *                  DESC - 0
     * @return iterator over keys for the specified direction
     */
    @Override
    public OffHeapKeyIterator keyIterator(OrderingDirection direction) {
        this.offHeapKeyIterator.setDirection(direction);
        return this.offHeapKeyIterator;
    }

    /***
     * @param keyEntryPointer - address of the key entry
     * @return iterator over values for the specified direction
     */
    @Override
    public OffHeapValueIterator valueIterator(long keyEntryPointer) {
        this.offHeapValueEntryIterator.reset(keyEntryPointer);
        return this.offHeapValueEntryIterator;
    }

    private int compareKeys(OffHeapComparator comparator, long keyAddress, long keySize, long entryAddress) {
        return comparator.compare(
                keyAddress,
                keySize,
                getKeyAddress(entryAddress),
                getKeyWrittenBytes(entryAddress)
        );
    }

    private void setKey(long address, long keyAddress, long keyLength, long keyAllocatedBytes) {
        UNSAFE.putLong(getKeyAddressOffset(address), keyAddress);
        UNSAFE.putLong(getKeyWrittenBytesAddressOffset(address), keyLength);
        UNSAFE.putLong(getKeyAllocatedBytesAddressOffset(address), keyAllocatedBytes);
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
        return this.put(keyAddress, keyWrittenBytes, keyAllocatedBytes,
                valueAddress, valueWrittenBytes, valueAllocatedBytes,
                this.offHeapKeyComparator
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
                    long valueAddress, long valueWrittenBytes, long valueAllocatedBytes, OffHeapComparator comparator) {
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

        UNSAFE.putLong(getValueAddressOffset(newValueEntryAddress), valueAddress);
        UNSAFE.putLong(getValueWrittenBytesOffset(newValueEntryAddress), valueWrittenBytes);
        UNSAFE.putLong(getValueAllocatedBytesOffset(newValueEntryAddress), valueAllocatedBytes);

        long valueEntryAddress = UNSAFE.getLong(valueEntryOffsetAddress);

        if (valueEntryAddress == 0L) { // If it is first element - setting link on him
            UNSAFE.putLong(valueEntryOffsetAddress, newValueEntryAddress);
            UNSAFE.putLong(getLastValueEntryAddressOffset(newValueEntryAddress), newValueEntryAddress); // Set last address
        } else {
            long lastValueEntryAddress = UNSAFE.getLong(getLastValueEntryAddressOffset(valueEntryAddress));

            if (lastValueEntryAddress > 0L) {
                UNSAFE.putLong(getNextValueEntryOffset(lastValueEntryAddress), newValueEntryAddress); // Link old last value with new
            }

            UNSAFE.putLong(getLastValueEntryAddressOffset(valueEntryAddress), newValueEntryAddress); // Set last address to key
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
        return this.getKeyEntry(keyAddress, keyWrittenBytes, keyAllocatedBytes, this.offHeapKeyComparator, createIfNotExists);
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
    public long getKeyEntry(long keyAddress, long keyWrittenBytes, long keyAllocatedBytes, OffHeapComparator comparator, boolean createIfNotExists) {
        if (this.rootAddress == 0L) {
            if (createIfNotExists) {
                this.rootAddress = this.memoryAllocator.allocate(Bits.LONG_SIZE_IN_BYTES);
            } else {
                return 0L;
            }
        }

        return getKeyEntry0(
                this.rootAddress,
                comparator == null ? this.offHeapKeyComparator : comparator,
                keyAddress,
                keyWrittenBytes,
                keyAllocatedBytes,
                createIfNotExists
        );
    }

    private long getRootKeyEntry(long rootAddress) {
        return UNSAFE.getLong(rootAddress);
    }

    private void setRootKeyEntry(long rootAddress, long keyEntryAddress) {
        UNSAFE.putLong(rootAddress, keyEntryAddress);
    }

    protected long getKeyEntry0(long rootAddress,
                                OffHeapComparator comparator,
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

            if (compareResult == 1) { //Our key is greater
                long rightAddress = getRightAddressOffset(address);

                if (UNSAFE.getLong(rightAddress) == 0) {
                    if (createIfNotExists) {
                        return assignNewLeaf(rootAddress, keyAddress, keyWrittenBytes, keyAllocatedBytes, address, rightAddress, RED, RIGHT);
                    } else {
                        return 0L;
                    }
                } else {
                    address = UNSAFE.getLong(rightAddress);
                }
            } else if (compareResult == -1) { //Our key is less
                long leftAddress = getLeftAddressOffset(address);

                if (UNSAFE.getLong(leftAddress) == 0) {
                    if (createIfNotExists) {
                        return assignNewLeaf(rootAddress, keyAddress, keyWrittenBytes, keyAllocatedBytes, address, leftAddress, RED, LEFT);
                    } else {
                        return 0L;
                    }
                } else {
                    address = UNSAFE.getLong(leftAddress);
                }
            } else { //Out key is the same
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

        UNSAFE.putLong(leafAddress, address);

        long parentAddress = getParentAddressOffset(address);
        UNSAFE.putLong(parentAddress, entryAddress);

        long sideOffset = getSideAddressOffset(address);
        UNSAFE.putByte(sideOffset, side);

        checkRedBlackConsistency(rootAddress, entryAddress, address, side);

        return address;
    }

    private void checkRedBlackConsistency(
            long rootAddress,
            long fatherAddress,
            long sonAddress,
            byte sonSide
    ) {
        if (fatherAddress == 0L) {
            blackRoot(rootAddress);
            return;
        }

        long grandFatherAddress;
        long grandFatherOffsetAddress = getParentAddressOffset(fatherAddress);

        if (grandFatherOffsetAddress > 0L) {
            grandFatherAddress = UNSAFE.getLong(grandFatherOffsetAddress);
        } else {
            return;
        }

        if (UNSAFE.getByte(getColorAddressOffset(fatherAddress)) == BLACK) {
            return;
        }

        byte fathersSide = UNSAFE.getByte(getSideAddressOffset(fatherAddress));
        long uncleAddress;

        if (fathersSide == LEFT) {
            long rightAddress = getRightAddressOffset(grandFatherAddress);
            uncleAddress = rightAddress > 0L ? UNSAFE.getLong(rightAddress) : 0L;
        } else {
            long leftAddress = getLeftAddressOffset(grandFatherAddress);
            uncleAddress = leftAddress > 0L ? UNSAFE.getLong(leftAddress) : 0L;
        }

        if (uncleAddress > 0L) { //Case 1 - red uncle
            if (case1(rootAddress, fatherAddress, grandFatherAddress, uncleAddress)) {
                return;
            }
        }

        //Case 2: Son's and father's side are different, uncle is black or absent
        if ((sonSide != fathersSide)) {
            case2(fatherAddress, sonAddress, sonSide, grandFatherAddress, fathersSide);

            //Switch father and son address
            long tmp = fatherAddress;
            fatherAddress = sonAddress;
            sonAddress = tmp;

            sonSide = UNSAFE.getByte(getSideAddressOffset(sonAddress));
            fathersSide = UNSAFE.getByte(getSideAddressOffset(fatherAddress));
        }

        //Case 3: Son's and father's side are the same, uncle is black or absent
        if (sonSide == fathersSide) {
            if (case3(rootAddress, fatherAddress, sonSide, grandFatherAddress, fathersSide)) {
                return;
            }
        }

        blackRoot(rootAddress);
    }

    private boolean case3(long rootAddress,
                          long fatherAddress,
                          byte sonSide,
                          long grandFatherAddress,
                          byte fathersSide) {
        if (grandFatherAddress == 0) {
            return true;
        }

        long grandFathersParentEntry;
        long parentOffsetAddress =
                getParentAddressOffset(grandFatherAddress);

        grandFathersParentEntry = parentOffsetAddress > 0L ? UNSAFE.getLong(parentOffsetAddress) : 0L;

        //Grandfather's migration
        if (grandFathersParentEntry == 0L) {
            UNSAFE.putLong(rootAddress, fatherAddress);

            UNSAFE.putLong(
                    getParentAddressOffset(fatherAddress), 0L);
        } else {
            byte grandFathersSide = UNSAFE.getByte(
                    getSideAddressOffset(grandFatherAddress));

            if (grandFathersSide == LEFT) {
                UNSAFE.putLong(
                        getLeftAddressOffset(grandFathersParentEntry), fatherAddress);
                UNSAFE.putByte(
                        getSideAddressOffset(fatherAddress), LEFT);//Set father's side
                UNSAFE.putLong(
                        getParentAddressOffset(fatherAddress), grandFathersParentEntry); // Father on grandFather's father
            } else {
                UNSAFE.putLong(
                        getRightAddressOffset(grandFathersParentEntry), fatherAddress);
                UNSAFE.putByte(
                        getSideAddressOffset(fatherAddress), RIGHT); //Set father's side
                UNSAFE.putLong(
                        getParentAddressOffset(fatherAddress), grandFathersParentEntry); // Father on grandFather's father
            }
        }

        long grandFathersLeftOffset =
                getLeftAddressOffset(grandFatherAddress);
        long grandFathersRightOffset =
                getRightAddressOffset(grandFatherAddress);

        //Father's migration
        long fathersLeftOffset =
                getLeftAddressOffset(fatherAddress);
        long fathersRightOffset =
                getRightAddressOffset(fatherAddress);

        long fatherLeftAddress = UNSAFE.getLong(fathersLeftOffset);
        long fatherRightAddress = UNSAFE.getLong(fathersRightOffset);

        // Father link to grandfather
        // Grandfather links to father's another child
        if (sonSide == LEFT) {
            UNSAFE.putLong(fathersRightOffset, grandFatherAddress);
            UNSAFE.putLong(grandFathersLeftOffset, fatherRightAddress);
            UNSAFE.putLong(
                    getParentAddressOffset(grandFatherAddress), fatherAddress); //GrandFather on father

            if (fatherRightAddress > 0L) {   // Set father's child to grandFather
                UNSAFE.putByte(getSideAddressOffset(fatherRightAddress), LEFT);
                UNSAFE.putLong(getParentAddressOffset(fatherRightAddress), grandFatherAddress);
            }

            UNSAFE.putByte(
                    getSideAddressOffset(grandFatherAddress), RIGHT);  //Set grandFather's side
        } else {
            UNSAFE.putLong(fathersLeftOffset, grandFatherAddress);
            UNSAFE.putLong(grandFathersRightOffset, fatherLeftAddress);
            UNSAFE.putLong(
                    getParentAddressOffset(grandFatherAddress), fatherAddress); //  GrandFather on father

            if (fatherLeftAddress > 0L) { // Set father's child to grandFather
                UNSAFE.putByte(getSideAddressOffset(fatherLeftAddress), RIGHT);
                UNSAFE.putLong(getParentAddressOffset(fatherLeftAddress), grandFatherAddress);
            }

            UNSAFE.putByte(
                    getSideAddressOffset(grandFatherAddress), LEFT); //Set grandFather's side
        }

        //Changing color
        UNSAFE.putByte(
                getColorAddressOffset(grandFatherAddress), RED); // GrandFather - red
        UNSAFE.putByte(
                getColorAddressOffset(fatherAddress), BLACK); // Father - black

        //Grandfather's side will become uncle's side
        UNSAFE.putByte(
                getSideAddressOffset(grandFatherAddress), fathersSide == LEFT ? RIGHT : LEFT);
        return false;
    }

    private void case2(long fatherAddress, long sonAddress, byte sonSide, long grandFatherAddress, byte fathersSide) {
        long grandFathersOffset = fathersSide == LEFT ?

                getLeftAddressOffset(grandFatherAddress) :

                getRightAddressOffset(grandFatherAddress);
        UNSAFE.putLong(grandFathersOffset, sonAddress);

        long sonLeftAddress =
                getLeftAddressOffset(sonAddress);
        long sonRightAddress =
                getRightAddressOffset(sonAddress);

        long sonsChild = sonSide == RIGHT ? UNSAFE.getLong(sonLeftAddress) : UNSAFE.getLong(sonRightAddress);

        //Son's left becomes father's right
        UNSAFE.putLong(sonSide == RIGHT ?
                        getRightAddressOffset(fatherAddress) :
                        getLeftAddressOffset(fatherAddress),
                sonsChild
        );

        if (sonsChild > 0L) {
            UNSAFE.putLong(getParentAddressOffset(sonsChild), fatherAddress);
            UNSAFE.putByte(getSideAddressOffset(sonsChild), sonSide);
        }

        // Son becomes parent of father
        UNSAFE.putLong(sonSide == RIGHT ? sonLeftAddress : sonRightAddress, fatherAddress);

        //Set new parents to father and son
        UNSAFE.putLong(getParentAddressOffset(fatherAddress), sonAddress);
        UNSAFE.putLong(getParentAddressOffset(sonAddress), grandFatherAddress);

        //Change son's side
        UNSAFE.putByte(
                getSideAddressOffset(sonAddress), sonSide == LEFT ? RIGHT : LEFT);
    }

    private boolean case1(long rootAddress, long fatherAddress, long grandFatherAddress, long uncleAddress) {
        byte uncleColor = UNSAFE.getByte(
                getColorAddressOffset(uncleAddress));

        if (uncleColor == RED) {
            UNSAFE.putByte(
                    getColorAddressOffset(uncleAddress), BLACK);    //Repaint uncle to black
            UNSAFE.putByte(
                    getColorAddressOffset(fatherAddress), BLACK);   //Repaint father to black

            if (grandFatherAddress > 0L) {
                UNSAFE.putByte(
                        getColorAddressOffset(grandFatherAddress), RED); //Repaint grand-father to red

                byte grandFatherSide = UNSAFE.getByte(
                        getSideAddressOffset(grandFatherAddress));
                long grandFatherParentOffsetAddress =
                        getParentAddressOffset(grandFatherAddress);
                long grandFatherParentAddress = grandFatherParentOffsetAddress > 0L ? UNSAFE.getLong(grandFatherParentOffsetAddress) : 0L;

                checkRedBlackConsistency(rootAddress, grandFatherParentAddress, grandFatherAddress, grandFatherSide); // Recursive call
            } else {
                long rootKeyEntry = getRootKeyEntry(rootAddress);
                if (rootKeyEntry > 0L) {
                    UNSAFE.putByte(getColorAddressOffset(rootKeyEntry), BLACK);
                }
            }

            return true;
        }
        return false;
    }

    private void blackRoot(long rootAddress) {
        long rootEntry = getRootKeyEntry(rootAddress);

        if (rootEntry > 0L) {
            UNSAFE.putByte(getColorAddressOffset(rootEntry), BLACK);
        }
    }

    @Override
    public long first(OrderingDirection direction) {
        if (this.rootAddress == 0) {
            return 0L;
        }

        return first(direction, getRootKeyEntry(this.rootAddress));
    }

    @Override
    public long first(OrderingDirection direction,
                      long keyEntry) {
        if (keyEntry == 0L) {
            return 0L;
        }

        long pointer = keyEntry;

        while (true) {
            long next;

            if (direction == ASC) {
                next = UNSAFE.getLong(
                        getLeftAddressOffset(pointer));
            } else {
                next = UNSAFE.getLong(
                        getRightAddressOffset(pointer));
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
    public long getNext(long pointer,
                        OrderingDirection direction) {
        long childAddress = direction == ASC ?
                getRightAddressOffset(pointer) : getLeftAddressOffset(pointer);

        if (childAddress > 0L) {
            long child = UNSAFE.getLong(childAddress);

            if (child > 0L) {
                return first(direction, child);
            }
        }

        byte side = UNSAFE.getByte(
                getSideAddressOffset(pointer));

        if (checkSide(side, direction)) {
            long parentAddress = UNSAFE.getLong(
                    getParentAddressOffset(pointer));

            if (parentAddress > 0L) {
                return parentAddress;
            }
        }

        while (!checkSide(side, direction)) {
            long parentAddress = UNSAFE.getLong(
                    getParentAddressOffset(pointer));

            if (parentAddress == 0) {
                return 0;
            } else {
                pointer = parentAddress;
                side = UNSAFE.getByte(
                        getSideAddressOffset(pointer));
            }
        }

        return UNSAFE.getLong(
                getParentAddressOffset(pointer));
    }

    @Override
    public long getKeyAddress(long keyEntryPointer) {
        if (keyEntryPointer == 0L) {
            return 0L;
        }

        if (getKeyAddressOffset(keyEntryPointer) > 0L) {
            return UNSAFE.getLong(
                    getKeyAddressOffset(keyEntryPointer));
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
            return UNSAFE.getLong(
                    getKeyWrittenBytesAddressOffset(keyEntryPointer));
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
            return UNSAFE.getLong(
                    getKeyAllocatedBytesAddressOffset(keyEntryPointer));
        } else {
            return 0L;
        }
    }

    @Override
    public void markKeyEntry(long keyEntryPointer, byte marker) {
        if (keyEntryPointer != 0L) {
            UNSAFE.putByte(getMerkerAddressOffset(keyEntryPointer), marker);
        }
    }

    @Override
    public byte getKeyEntryMarker(long keyEntryPointer) {
        if (keyEntryPointer != 0L) {
            return UNSAFE.getByte(getMerkerAddressOffset(keyEntryPointer));
        } else {
            return 0;
        }
    }

    @Override
    public long count() {
        if (this.rootAddress > 0L) {
            long rootKeyEntry = getRootKeyEntry(this.rootAddress);
            return rootKeyEntry == 0 ? 0 : count0(rootKeyEntry);
        }

        return 0;
    }

    @Override
    public boolean validate() {
        if (this.rootAddress > 0L) {
            long rootKeyEntry = getRootKeyEntry(this.rootAddress);
            return validate0(rootKeyEntry, 0L);
        } else {
            return true;
        }
    }

    private boolean validate0(long pointer, long father) {
        long parentAddressOffset =
                getParentAddressOffset(pointer);
        long sideOffset =
                getSideAddressOffset(pointer);
        long parentAddress = UNSAFE.getLong(parentAddressOffset);

        if (father != parentAddress) {
            return false;
        }

        if (parentAddress > 0L) {
            byte side = UNSAFE.getByte(sideOffset);
            long childAddress = UNSAFE.getLong(side == LEFT ?
                    getLeftAddressOffset(parentAddress) :
                    getRightAddressOffset(parentAddress));

            if (childAddress != pointer) {
                return false;
            }
        } else {
            if (pointer != getRootKeyEntry(this.rootAddress)) {
                return false;
            }
        }

        if (validateChild(pointer, LEFT)) {
            return false;
        }

        if (validateChild(pointer, RIGHT)) {
            return false;
        }

        return true;
    }

    private boolean validateChild(long pointer, byte side) {
        long childAddress = UNSAFE.getLong(side == LEFT ?
                        getLeftAddressOffset(pointer) :
                        (getRightAddressOffset(pointer))
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

        if (UNSAFE.getLong(getLeftAddressOffset(pointer)) > 0L) {
            counter += count0(UNSAFE.getLong(getLeftAddressOffset(pointer)));
        }

        if (UNSAFE.getLong(getRightAddressOffset(pointer)) > 0L) {
            counter += count0(UNSAFE.getLong(getRightAddressOffset(pointer)));
        }

        return counter;
    }

    @Override
    public void dispose() {
        if (this.rootAddress > 0L) {
            dispose0(getRootKeyEntry(this.rootAddress));
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
        if (this.rootAddress > 0L) {
            long rootKeyEntry = getRootKeyEntry(this.rootAddress);
            release0(rootKeyEntry, false);
            freeRoot();
        }
    }

    protected void freeRoot() {
        this.memoryAllocator.free(this.rootAddress, Bits.LONG_SIZE_IN_BYTES);
        this.rootAddress = 0L;
    }

    protected void release0(long keyEntryAddress, boolean releasePayLoad) {
        if (UNSAFE.getLong(
                getLeftAddressOffset(keyEntryAddress)) > 0L) {
            release0(UNSAFE.getLong(
                    getLeftAddressOffset(keyEntryAddress)), releasePayLoad);
        }

        if (UNSAFE.getLong(
                getRightAddressOffset(keyEntryAddress)) > 0L) {
            release0(UNSAFE.getLong(
                    getRightAddressOffset(keyEntryAddress)), releasePayLoad);
        }

        releaseKeyEntry(keyEntryAddress, releasePayLoad);
    }

    protected void releaseKeyEntry(long keyEntryAddress, boolean releasePayLoad) {
        releaseKeyEntry(keyEntryAddress, releasePayLoad, true);
    }

    protected void releaseKeyEntry(long keyEntryAddress, boolean releasePayLoad, boolean releaseValue) {
        if (keyEntryAddress > 0L) {
            //Release Key PayLoad
            long keyAddress =
                    getKeyAddress(keyEntryAddress);

            if ((keyAddress > 0L) && (releasePayLoad)) {
                this.memoryAllocator.free(keyAddress,
                        getKeyAllocatedBytes(keyEntryAddress));
            }

            if (releaseValue) {
                //Release Values
                releaseValue(keyEntryAddress, releasePayLoad);
            }

            //Release Key Entry
            this.memoryAllocator.free(keyEntryAddress, KEY_ENTRY_SIZE);
        }
    }

    protected void releaseValue(long keyEntryAddress, boolean releasePayLoad) {
        long valueEntryAddressOffset =
                getValueEntryAddressOffset(keyEntryAddress);
        long valueEntryAddress = UNSAFE.getLong(valueEntryAddressOffset);

        while (valueEntryAddress > 0L) {
            //Release Value PayLoad
            if (releasePayLoad) {
                releaseValuePayLoad(valueEntryAddress);
            }

            long oldValueEntryAddress = valueEntryAddress;

            valueEntryAddress = UNSAFE.getLong(
                    getNextValueEntryOffset(valueEntryAddress));

            if (oldValueEntryAddress > 0L) {
                //Release Value Entry
                this.memoryAllocator.free(oldValueEntryAddress, VALUE_ENTRY_SIZE);
            }
        }
    }

    protected void releaseValuePayLoad(long valueEntryAddress) {
        long valueAddressOffset =
                getValueAddressOffset(valueEntryAddress);

        if (valueAddressOffset > 0L) {
            long valueAddress = UNSAFE.getLong(valueAddressOffset);

            if (valueAddress > 0L) {
                long allocatedBytes = 0;

                if (getValueAllocatedBytesOffset(valueEntryAddress) > 0L) {
                    allocatedBytes = UNSAFE.getLong(
                            getValueAllocatedBytesOffset(valueEntryAddress));
                }

                this.memoryAllocator.free(valueAddress, allocatedBytes);
            }
        }
    }
}