package com.hazelcast.elastic.offheapstorage.sorted.secondarykey;

import com.hazelcast.elastic.offheapstorage.OffHeapComparator;
import com.hazelcast.elastic.offheapstorage.iterator.secondarykey.OffHeapSecondaryKeyIterator;
import com.hazelcast.elastic.offheapstorage.iterator.secondarykey.OffHeapSecondaryKeyIteratorImpl;
import com.hazelcast.elastic.offheapstorage.sorted.OffHeapKeyValueRedBlackTreeStorage;
import com.hazelcast.elastic.offheapstorage.sorted.OrderingDirection;
import com.hazelcast.memory.MemoryAllocator;

/**
 * Specialization of off-heap red-black tree to secondary key storage
 */
public class OffHeapSecondaryKeyValueRedBlackTreeStorage extends OffHeapKeyValueRedBlackTreeStorage
        implements OffHeapSecondaryKeyValueSortedStorage {

    private long lastInsertedSecondaryKeyEntry;
    private final OffHeapComparator offHeapSecondaryKeyComparator;
    private final OffHeapSecondaryKeyIterator offHeapSecondaryKeyIterator;

    public OffHeapSecondaryKeyValueRedBlackTreeStorage(MemoryAllocator memoryAllocator, OffHeapComparator offHeapKeyComparator,
                                                       OffHeapComparator offHeapSecondaryKeyComparator) {
        super(memoryAllocator, offHeapKeyComparator);
        this.offHeapSecondaryKeyComparator = offHeapSecondaryKeyComparator;
        this.offHeapSecondaryKeyIterator = new OffHeapSecondaryKeyIteratorImpl(this);
    }

    protected long insertSecondaryKey(long secondaryKeyAddress, long secondaryKeyWrittenBytes, long secondaryKeyAllocatedBytes,
                                      long keyEntryAddress, OffHeapComparator secondaryKeyComparator) {
        long secondaryKeyEntryOffsetAddress = getValueEntryAddressOffset(keyEntryAddress);

        long secondaryKeyEntryAddress = getKeyEntry0(
                secondaryKeyEntryOffsetAddress,
                secondaryKeyComparator == null ? offHeapSecondaryKeyComparator : secondaryKeyComparator,
                secondaryKeyAddress,
                secondaryKeyWrittenBytes,
                secondaryKeyAllocatedBytes,
                true
        );

        if (MEMORY_ACCESSOR.getLong(secondaryKeyEntryOffsetAddress) == 0L) {
            // Insert rootAddress's address here
            MEMORY_ACCESSOR.putLong(secondaryKeyEntryOffsetAddress, secondaryKeyEntryAddress);
        }

        return secondaryKeyEntryAddress;
    }

    /**
     * Looking for key equal to buffer by address;
     * keyAddress with params keyWrittenBytes and  keyAllocatedBytes;
     * In case if found - append value represented by;
     * valueAddress with params  valueWrittenBytes and  valueAllocatedBytes to the chain of key's values;
     * <p/>
     * Perform the same for the secondaryKey address and bind new value to the secondary key
     * <p/>
     * In case if not-found - adds new key to the corresponding place in the tree and assign value to this 'new' key;
     *
     * @param keyAddress                 - address of the key;
     * @param keyWrittenBytes            - amount of written bytes of the key;
     * @param keyAllocatedBytes          - amount of allocated bytes of the key;
     * @param secondaryKeyAddress        - address of the secondaryKey;
     * @param secondaryKeyWrittenBytes   - amount of written bytes of the secondary key;
     * @param secondaryKeyAllocatedBytes - amount of allocated bytes of the secondary key;
     * @param valueAddress               - address of the value;
     * @param valueWrittenBytes          - amount of written bytes of the value;
     * @param valueAllocatedBytes        - amount of allocated bytes of the value;
     * @return address of the key entry;
     */
    @Override
    @SuppressWarnings("checkstyle:parameternumber")
    public long put(long keyAddress, long keyWrittenBytes, long keyAllocatedBytes,
                    long secondaryKeyAddress, long secondaryKeyWrittenBytes, long secondaryKeyAllocatedBytes,
                    long valueAddress, long valueWrittenBytes, long valueAllocatedBytes) {
        return put(keyAddress, keyWrittenBytes, keyAllocatedBytes,
                secondaryKeyAddress, secondaryKeyWrittenBytes, secondaryKeyAllocatedBytes,
                valueAddress, valueWrittenBytes, valueAllocatedBytes,
                offHeapKeyComparator, offHeapSecondaryKeyComparator
        );
    }

    /**
     * Looking for key equal to buffer by address;
     * keyAddress with params keyWrittenBytes and  keyAllocatedBytes;
     * In case if found - append value represented by;
     * valueAddress with params  valueWrittenBytes and  valueAllocatedBytes to the chain of key's values;
     * <p/>
     * Perform the same for the secondaryKey address and bind new value to the secondary key
     * <p/>
     * In case if not-found - adds new key to the corresponding place in the tree and assign value to this 'new' key;
     *
     * @param keyAddress                 - address of the key;
     * @param keyWrittenBytes            - amount of written bytes of the key;
     * @param keyAllocatedBytes          - amount of allocated bytes of the key;
     * @param secondaryKeyAddress        - address of the secondaryKey;
     * @param secondaryKeyWrittenBytes   - amount of written bytes of the secondary key;
     * @param secondaryKeyAllocatedBytes - amount of allocated bytes of the secondary key;
     * @param valueAddress               - address of the value;
     * @param valueWrittenBytes          - amount of written bytes of the value;
     * @param valueAllocatedBytes        - amount of allocated bytes of the value;
     * @param primaryKeyComparator       - comparator to be used for the primary keys,
     *                                   if null - default storage's comparator to be used;
     * @param secondaryKeyComparator     - comparator to be used for the secondary keys,
     *                                   if null - default storage's comparator to be used;
     * @return address of the key entry;
     */
    @Override
    @SuppressWarnings("checkstyle:parameternumber")
    public long put(long keyAddress, long keyWrittenBytes, long keyAllocatedBytes,
                    long secondaryKeyAddress, long secondaryKeyWrittenBytes, long secondaryKeyAllocatedBytes,
                    long valueAddress, long valueWrittenBytes, long valueAllocatedBytes,
                    OffHeapComparator primaryKeyComparator, OffHeapComparator secondaryKeyComparator) {
        long keyEntryAddress = getKeyEntry(keyAddress, keyWrittenBytes, keyAllocatedBytes, primaryKeyComparator, true);

        long secondaryKeyEntry = insertSecondaryKey(
                secondaryKeyAddress,
                secondaryKeyWrittenBytes,
                secondaryKeyAllocatedBytes,
                keyEntryAddress,
                secondaryKeyComparator
        );

        if (secondaryKeyEntry > 0L) {
            insertValue(
                    valueAddress,
                    valueWrittenBytes,
                    valueAllocatedBytes,
                    secondaryKeyEntry
            );
        }

        lastInsertedSecondaryKeyEntry = secondaryKeyEntry;
        return keyEntryAddress;
    }

    private long getSecondaryKeyAddress(long keyEntryAddress) {
        return getValueEntryAddress(keyEntryAddress);
    }

    protected void releaseKeyEntry(long keyEntryAddress, boolean releasePayLoad) {
        if (keyEntryAddress > 0L) {
            // Release Secondary key
            releaseSecondaryKey(getSecondaryKeyAddress(keyEntryAddress), releasePayLoad);
            releaseKeyEntry(keyEntryAddress, releasePayLoad, false);
        }
    }

    protected void releaseSecondaryKey(long secondaryKeyEntryAddress, boolean releasePayLoad) {
        if (secondaryKeyEntryAddress > 0L) {
            if (MEMORY_ACCESSOR.getLong(
                    getLeftAddressOffset(secondaryKeyEntryAddress)) > 0L) {
                releaseSecondaryKey(MEMORY_ACCESSOR.getLong(
                        getLeftAddressOffset(secondaryKeyEntryAddress)), releasePayLoad);
            }

            if (MEMORY_ACCESSOR.getLong(
                    getRightAddressOffset(secondaryKeyEntryAddress)) > 0L) {
                releaseSecondaryKey(MEMORY_ACCESSOR.getLong(
                        getRightAddressOffset(secondaryKeyEntryAddress)), releasePayLoad);
            }

            releaseKeyEntry(secondaryKeyEntryAddress, releasePayLoad, true);
        }
    }

    @Override
    public OffHeapSecondaryKeyIterator secondaryKeyIterator(long keyEntryPointer, OrderingDirection orderingDirection) {
        offHeapSecondaryKeyIterator.setKeyEntry(keyEntryPointer);
        offHeapSecondaryKeyIterator.setDirection(orderingDirection);
        return offHeapSecondaryKeyIterator;
    }

    @Override
    public long firstSecondaryKeyEntry(OrderingDirection direction, long keyEntryAddress) {
        return first(direction, getSecondaryKeyAddress(keyEntryAddress));
    }

    @Override
    public long getLastInsertedSecondaryKeyEntry() {
        return lastInsertedSecondaryKeyEntry;
    }

    @Override
    public long firstSecondaryKeyEntry(OrderingDirection direction, long keyEntryAddress, long secondaryKeyEntry) {
        return first(direction, secondaryKeyEntry);
    }
}
