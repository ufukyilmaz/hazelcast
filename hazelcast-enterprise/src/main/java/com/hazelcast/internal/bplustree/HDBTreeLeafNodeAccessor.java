package com.hazelcast.internal.bplustree;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

import static com.hazelcast.internal.bplustree.CompositeKeyComparison.greater;
import static com.hazelcast.internal.bplustree.CompositeKeyComparison.less;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * An utility class to support for the B+Tree leaf node access operations.
 */
final class HDBTreeLeafNodeAccessor extends HDBTreeNodeBaseAccessor {

    private static final int OFFSET_ADDR_BACK = OFFSET_NODE_BASE_DATA;
    private static final int OFFSET_ADDR_FORW = OFFSET_ADDR_BACK + 8;
    private static final int OFFSET_LEAF_ENTRIES = OFFSET_ADDR_FORW + 8;

    HDBTreeLeafNodeAccessor(LockManager lockManager,
                            EnterpriseSerializationService ess,
                            BPlusTreeKeyComparator keyComparator,
                            BPlusTreeKeyAccessor keyAccessor,
                            MemoryAllocator keyAllocator,
                            MemoryAllocator indexAllocator,
                            int nodeSize,
                            NodeSplitStrategy nodeSplitStrategy) {
        super(lockManager, ess, keyComparator, keyAccessor, keyAllocator, indexAllocator,
                nodeSize, nodeSplitStrategy);
    }

    @Override
    long newNodeLocked(LockingContext lockingContext) {
        long address = super.newNodeLocked(lockingContext);
        setBackNode(address, NULL_ADDRESS);
        setForwNode(address, NULL_ADDRESS);
        return address;
    }

    @Override
    int getOffsetEntries() {
        return OFFSET_LEAF_ENTRIES;
    }

    @Override
    boolean isNodeFull(long nodeAddr) {
        int keysCount = getKeysCount(nodeAddr);
        return (nodeSize - getOffsetEntries() - keysCount * SLOT_ENTRY_SIZE) < SLOT_ENTRY_SIZE;
    }

    long getBackNode(long nodeAddr) {
        return AMEM.getLong(nodeAddr + OFFSET_ADDR_BACK);
    }

    void setBackNode(long nodeAddr, long backAddr) {
        AMEM.putLong(nodeAddr + OFFSET_ADDR_BACK, backAddr);
    }

    long getForwNode(long nodeAddr) {
        return AMEM.getLong(nodeAddr + OFFSET_ADDR_FORW);
    }

    void setForwNode(long nodeAddr, long forwAddr) {
        AMEM.putLong(nodeAddr + OFFSET_ADDR_FORW, forwAddr);
    }

    /**
     * Returns lower bound slot defined by the indexKey/entryKey in the leaf node.
     *
     * @param nodeAddr the leaf node address
     * @param indexKey the index key
     * @param entryKey the entry key
     * @return the lower bound slot
     */
    int lowerBound(long nodeAddr, Comparable indexKey, NativeMemoryData entryKey) {
        assert indexKey != null;

        int lower = 0;
        int upper = getKeysCount(nodeAddr) - 1;
        for (; ; ) {
            if (upper < lower) {
                return lower;
            }
            int mid = (upper + lower) / 2;

            CompositeKeyComparison cmp = compareKeys(indexKey, entryKey, nodeAddr, mid);
            if (less(cmp)) {
                upper = mid - 1;
            } else if (greater(cmp)) {
                lower = mid + 1;
            } else {
                return mid;
            }
        }
    }

    /**
     * Inserts the key-value pair into the leaf keeping the entries ordered by indexKey/entryKey
     *
     * @param nodeAddr the node address
     * @param indexKey the index key
     * @param entryKey the entry key
     * @param value    the value
     * @return the old value if it exists, {@code null} otherwise
     */
    NativeMemoryData insert(long nodeAddr, Comparable indexKey, NativeMemoryData entryKey, MemoryBlock value) {
        int keysCount = getKeysCount(nodeAddr);

        assert !isNodeFull(nodeAddr) && keysCount >= 0;

        // Do binary search in the leaf node
        int lower = 0;
        int upper = keysCount - 1;
        int mid = 0;
        int slot = 0;
        boolean found = false;
        for (; ; ) {
            if (upper < lower) {
                slot = lower;
                break;
            }
            mid = (upper + lower) / 2;

            CompositeKeyComparison cmp = compareKeys(indexKey, entryKey, nodeAddr, mid);
            if (less(cmp)) {
                upper = mid - 1;
            } else if (greater(cmp)) {
                lower = mid + 1;
            } else {
                // Found the key
                slot = mid;
                found = true;
                break;
            }
        }

        if (slot < keysCount && found) {
            // Replace the existing value
            NativeMemoryData oldValue = getValue(nodeAddr, slot);
            setValue(nodeAddr, slot, value);
            incSequenceCounter(nodeAddr);
            return oldValue;
        }

        // Convert indexKey into NATIVE format before updating B+tree node
        // to make sure OOME doesn't leave the B+tree
        // in a corrupted state.
        long indexKeyAddr = clonedIndexKeyAddr(indexKey);

        // Shift the slots to make space for the new entry
        AMEM.copyMemory(getSlotAddr(nodeAddr, slot), getSlotAddr(nodeAddr, slot + 1),
                (keysCount - slot) * SLOT_ENTRY_SIZE);

        // Set the new slot
        setIndexKey(nodeAddr, slot, indexKeyAddr);
        setEntryKey(nodeAddr, slot, entryKey);
        setValue(nodeAddr, slot, value);

        setKeysCount(nodeAddr, keysCount + 1);
        incSequenceCounter(nodeAddr);
        return null;
    }

    /**
     * Removes the key-value pair defined by indexKey/entryKey from the leaf node.
     * Disposes indexKey off-heap memory "owned" by the index.
     *
     * @param nodeAddr the node address
     * @param indexKey the index key
     * @param entryKey the entry key
     * @return the old value if it exists, {@code null} otherwise
     */
    NativeMemoryData remove(long nodeAddr, Comparable indexKey, NativeMemoryData entryKey) {
        NativeMemoryData oldValue = null;
        int keysCount = getKeysCount(nodeAddr);

        if (keysCount > 0) {
            int lower = 0;
            int upper = keysCount - 1;
            int mid;
            for (; ; ) {
                if (upper < lower) {
                    return null;
                }
                mid = (upper + lower) / 2;

                CompositeKeyComparison cmp = compareKeys(indexKey, entryKey, nodeAddr, mid);
                if (less(cmp)) {
                    upper = mid - 1;
                } else if (greater(cmp)) {
                    lower = mid + 1;
                } else {
                    // Found a match
                    break;
                }
            }

            assert mid <= keysCount;
            oldValue = getValue(nodeAddr, mid);
            long oldIndexKeyAddr = getIndexKeyAddr(nodeAddr, mid);
            AMEM.copyMemory(getSlotAddr(nodeAddr, mid + 1), getSlotAddr(nodeAddr, mid),
                    (keysCount - mid - 1) * SLOT_ENTRY_SIZE);
            setKeysCount(nodeAddr, keysCount - 1);
            incSequenceCounter(nodeAddr);
            keyAccessor.disposeNativeData(oldIndexKeyAddr, getKeyAllocator());
        }
        return oldValue;
    }

    /**
     * Splits the node and moves part of the slots to the new node.
     *
     * @param nodeAddr       the address  of the node to be splitted
     * @param lockingContext the locking context
     * @return the address of the new leaf node which is write locked
     */
    long split(long nodeAddr, LockingContext lockingContext) {
        // Assert leaf node
        assert getNodeLevel(nodeAddr) == 0;
        long newLeaf = newNodeLocked(lockingContext);
        int keysCount = getKeysCount(nodeAddr);
        int newLeafKeysCount = nodeSplitStrategy.getNewNodeKeysCount(keysCount);
        keysCount -= newLeafKeysCount;
        setKeysCount(newLeaf, newLeafKeysCount);
        setKeysCount(nodeAddr, keysCount);
        AMEM.copyMemory(getSlotAddr(nodeAddr, keysCount), getSlotAddr(newLeaf, 0), newLeafKeysCount * SLOT_ENTRY_SIZE);
        incSequenceCounter(nodeAddr);
        incSequenceCounter(newLeaf);
        return newLeaf;
    }

    /**
     * @param nodeAddr the node address
     * @return a slot used as a separation key
     */
    int getSeparationSlot(long nodeAddr) {
        // Assert leaf node
        assert getNodeLevel(nodeAddr) == 0;
        int keysCount = getKeysCount(nodeAddr);
        return keysCount - nodeSplitStrategy.getNewNodeKeysCount(keysCount) - 1;
    }

    /**
     * Disposes indexKey components on the node.
     */
    void disposeOwnedData(long nodeAddr) {
        int keysCount = getKeysCount(nodeAddr);
        assert getNodeLevel(nodeAddr) == 0;

        for (int slot = 0; slot < keysCount; ++slot) {
            long indexKeyAddr = getIndexKeyAddr(nodeAddr, slot);
            keyAccessor.disposeNativeData(indexKeyAddr, getKeyAllocator());
        }
    }

    private long clonedIndexKeyAddr(Comparable indexKey) {
        return keyAccessor.convertToNativeData(indexKey, getKeyAllocator());
    }
}
