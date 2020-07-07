package com.hazelcast.internal.bplustree;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

import static com.hazelcast.internal.bplustree.CompositeKeyComparison.greater;
import static com.hazelcast.internal.bplustree.CompositeKeyComparison.less;
import static com.hazelcast.internal.bplustree.DefaultBPlusTreeKeyAccessor.cloneNativeMemory;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * An utility class to support for the B+Tree inner node access operations.
 */
final class HDBTreeInnerNodeAccessor extends HDBTreeNodeBaseAccessor {

    HDBTreeInnerNodeAccessor(LockManager lockManager, EnterpriseSerializationService ess, BPlusTreeKeyComparator keyComparator,
                             BPlusTreeKeyAccessor keyAccessor, MemoryAllocator keyAllocator,
                             MemoryAllocator indexAllocator,
                             int nodeSize, NodeSplitStrategy nodeSplitStrategy) {
        super(lockManager, ess, keyComparator, keyAccessor, keyAllocator, indexAllocator, nodeSize, nodeSplitStrategy);
    }

    @Override
    int getOffsetEntries() {
        return OFFSET_NODE_BASE_DATA;
    }

    @Override
    boolean isNodeFull(long nodeAddr) {
        int keysCount = getKeysCount(nodeAddr);
        return (nodeSize - getOffsetEntries() - (keysCount + 1) * SLOT_ENTRY_SIZE) < SLOT_ENTRY_SIZE;
    }

    /**
     * Returns address of the child node to search for the indexKey/entryKey
     *
     * @param nodeAddr the parent node address
     * @param indexKey the index key
     * @param entryKey the entry key
     * @return the address  of the child node
     */
    long getChildNode(long nodeAddr, Comparable indexKey, Data entryKey) {
        int slot = lowerBound(nodeAddr, indexKey, entryKey);
        return getValueAddr(nodeAddr, slot);
    }

    /**
     * Returns the lower bound slot defined by the indexKey/entryKey in the inner node.
     *
     * @param nodeAddr the inner node address
     * @param indexKey the index key
     * @param entryKey the entry key
     * @return the lower bound slot
     */
    int lowerBound(long nodeAddr, Comparable indexKey, Data entryKey) {
        int keysCount = getKeysCount(nodeAddr);
        int lower = 0;
        int upper = keysCount - 1;
        int mid;
        for (; ; ) {
            if (upper < lower) {
                return lower;
            }
            mid = (upper + lower) / 2;

            CompositeKeyComparison cmp = compareKeys(indexKey, entryKey, nodeAddr, mid);
            if (less(cmp)) {
                upper = mid - 1;
            } else if (greater(cmp)) {
                lower = mid + 1;
            } else {
                // Found a match
                return mid;
            }
        }
    }

    /**
     * Splits the inner node and moves part of the slots to the new inner node.
     *
     * @param nodeAddr       the address of the node to split
     * @param lockingContext the locking context
     * @return the address  of the new write locked node
     */
    long split(long nodeAddr, LockingContext lockingContext) {
        int level = getNodeLevel(nodeAddr);
        assert level > 0;
        long newInnerAddr = newNodeLocked(lockingContext);
        int keysCount = getKeysCount(nodeAddr);
        int newInnerKeysCount = nodeSplitStrategy.getNewNodeKeysCount(keysCount);
        keysCount = keysCount - newInnerKeysCount - 1;
        setKeysCount(newInnerAddr, newInnerKeysCount);
        setKeysCount(nodeAddr, keysCount);
        setNodeLevel(newInnerAddr, level);

        AMEM.copyMemory(getSlotAddr(nodeAddr, keysCount + 1), getSlotAddr(newInnerAddr, 0),
                (newInnerKeysCount + 1) * SLOT_ENTRY_SIZE);

        incSequenceCounter(nodeAddr);
        incSequenceCounter(newInnerAddr);
        return newInnerAddr;
    }

    /**
     * Inserts child reference to the empty inner node.
     *
     * @param nodeAddr  the node address
     * @param childAddr the child address
     */
    void insert(long nodeAddr, long childAddr) {
        assert getKeysCount(nodeAddr) == 0;
        // Insert the only child pointer
        setIndexKey(nodeAddr, 0, NULL_ADDRESS);
        setEntryKey(nodeAddr, 0, NULL_ADDRESS);
        setValue(nodeAddr, 0, childAddr);
        incSequenceCounter(nodeAddr);
    }

    /**
     * Inserts a new slot defined by the indexKey/entryKey into the inner node.
     *
     * @param nodeAddr     the iner node address
     * @param indexKeyAddr the index key address
     * @param entryKeyAddr the entry key address
     * @param childAddr    the child address
     */
    void insert(long nodeAddr, long indexKeyAddr, long entryKeyAddr, long childAddr) {
        assert !isNodeFull(nodeAddr);
        int keysCount = getKeysCount(nodeAddr);
        NativeMemoryData entryKeyData = new NativeMemoryData().reset(entryKeyAddr);
        Comparable indexKey = keyAccessor.convertToObject(indexKeyAddr);

        int slot = lowerBound(nodeAddr, indexKey, entryKeyData);

        AMEM.copyMemory(getSlotAddr(nodeAddr, slot), getSlotAddr(nodeAddr, slot + 1), (keysCount - slot + 1) * SLOT_ENTRY_SIZE);

        setIndexKey(nodeAddr, slot, indexKeyAddr);
        setEntryKey(nodeAddr, slot, entryKeyAddr);
        setValue(nodeAddr, slot, getValue(nodeAddr, slot + 1));
        setValue(nodeAddr, slot + 1, childAddr);
        setKeysCount(nodeAddr, keysCount + 1);
        incSequenceCounter(nodeAddr);
    }

    /**
     * Removes a key-value pair defined by the indexKey/entryKey from the inner node
     *
     * @param nodeAddr the address of the inner node
     * @param indexKey the index key
     * @param entryKey the entry key
     */
    void remove(long nodeAddr, Comparable indexKey, NativeMemoryData entryKey) {
        int keysCount = getKeysCount(nodeAddr);
        assert getNodeLevel(nodeAddr) >= 1;
        assert keysCount >= 1;
        int slot = lowerBound(nodeAddr, indexKey, entryKey);

        // Dispose indexKey/entryKey components, if that is not the trailing slot without keys
        if (slot < keysCount) {
            disposeSlotData(nodeAddr, slot);
        } else {
            assert slot == keysCount;
            // Dispose the indexKey/entryKey of the previous slot;
            // Tt will be trailing slot after update.
            disposeSlotData(nodeAddr, slot - 1);
        }

        AMEM.copyMemory(getSlotAddr(nodeAddr, slot + 1), getSlotAddr(nodeAddr, slot), (keysCount - slot) * SLOT_ENTRY_SIZE);
        setKeysCount(nodeAddr, keysCount - 1);
        incSequenceCounter(nodeAddr);
        assert getValueAddr(nodeAddr, 0) != NULL_ADDRESS;
    }

    private void disposeSlotData(long nodeAddr, int slot) {
        long oldIndexKeyAddr = getIndexKeyAddr(nodeAddr, slot);
        keyAccessor.disposeNativeData(oldIndexKeyAddr, getKeyAllocator());
        long oldEntryKeyAddr = getEntryKeyAddr(nodeAddr, slot);
        NativeMemoryData oldEntryKeyData = new NativeMemoryData().reset(oldEntryKeyAddr);
        ess.disposeData(oldEntryKeyData, getKeyAllocator());
        setIndexKey(nodeAddr, slot, NULL_ADDRESS);
        setEntryKey(nodeAddr, slot, NULL_ADDRESS);
    }

    /**
     * Disposes indexKey/entryKey components
     */
    void disposeOwnedData(long nodeAddr) {
        int keysCount = getKeysCount(nodeAddr);
        assert getNodeLevel(nodeAddr) > 0;

        for (int slot = 0; slot < keysCount; ++slot) {
            long indexKeyAddr = getIndexKeyAddr(nodeAddr, slot);
            long entryKeyAddr = getEntryKeyAddr(nodeAddr, slot);

            keyAccessor.disposeNativeData(indexKeyAddr, getKeyAllocator());

            NativeMemoryData entryKeyData = new NativeMemoryData().reset(entryKeyAddr);
            ess.disposeData(entryKeyData, getKeyAllocator());
        }
    }

    /**
     * Disposes all addresses to the inner node's key allocator
     *
     * @param addresses the addresses to dispose
     */
    void disposeAddresses(Long... addresses) {
        HazelcastException caught = null;

        for (Long address : addresses) {
            try {
                keyAccessor.disposeNativeData(address, getKeyAllocator());
            } catch (HazelcastException exception) {
                caught = exception;
            }
        }
        if (caught != null) {
            throw caught;
        }
    }

    long clonedIndexKeyAddr(long indexKeyAddr) {
        return keyAccessor.convertToNativeData(indexKeyAddr, getKeyAllocator());
    }

    long clonedEntryKeyAddr(long entryKeyAddr) {
        NativeMemoryData entryKeyData = new NativeMemoryData().reset(entryKeyAddr);
        return cloneNativeMemory(entryKeyData, getKeyAllocator()).address();
    }
}
