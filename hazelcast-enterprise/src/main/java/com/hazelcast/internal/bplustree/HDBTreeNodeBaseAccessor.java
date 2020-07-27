package com.hazelcast.internal.bplustree;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

import static com.hazelcast.internal.bplustree.CompositeKeyComparison.INDEX_KEY_EQUAL_ENTRY_KEY_GREATER;
import static com.hazelcast.internal.bplustree.CompositeKeyComparison.INDEX_KEY_EQUAL_ENTRY_KEY_LESS;
import static com.hazelcast.internal.bplustree.CompositeKeyComparison.INDEX_KEY_GREATER;
import static com.hazelcast.internal.bplustree.CompositeKeyComparison.INDEX_KEY_LESS;
import static com.hazelcast.internal.bplustree.CompositeKeyComparison.KEYS_EQUAL;
import static com.hazelcast.internal.bplustree.HDBPlusTree.PLUS_INFINITY_ENTRY_KEY;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * An utility class to support for the B+Tree node access operations common for both leaf and inner nodes.
 */
@SuppressWarnings("checkstyle:MethodCount")
abstract class HDBTreeNodeBaseAccessor {

    static final int SLOT_ENTRY_SIZE = 24;

    static final int OFFSET_LOCK_STATE = 0;
    static final int OFFSET_SEQUENCE_NUMBER = OFFSET_LOCK_STATE + 8;
    static final int OFFSET_LEVEL = OFFSET_SEQUENCE_NUMBER + 8;
    static final int OFFSET_KEYS_COUNT = OFFSET_LEVEL + 1;
    static final int OFFSET_NODE_BASE_DATA = OFFSET_KEYS_COUNT + 2;

    static final int OFFSET_NODE_CONTENT = OFFSET_LEVEL;
    static final int MAX_LEVEL = 0xFF;
    static final int MAX_NODE_SIZE = 0xFFFFF;
    // The node has to accommodate at least 3 slots (3 * 24 = 72 bytes) and node header (35 bytes)
    static final int MIN_NODE_SIZE = 128;

    final LockManager lockManager;
    final EnterpriseSerializationService ess;
    final BPlusTreeKeyComparator keyComparator;
    final BPlusTreeKeyAccessor keyAccessor;
    final MemoryAllocator keyAllocator;
    final MemoryAllocator btreeAllocator;
    final int nodeSize;
    NodeSplitStrategy nodeSplitStrategy;

    HDBTreeNodeBaseAccessor(LockManager lockManager, EnterpriseSerializationService ess, BPlusTreeKeyComparator keyComparator,
                            BPlusTreeKeyAccessor keyAccessor, MemoryAllocator keyAllocator, MemoryAllocator btreeAllocator,
                            int nodeSize, NodeSplitStrategy nodeSplitStrategy) {
        this.lockManager = lockManager;
        this.ess = ess;
        this.keyComparator = keyComparator;
        this.keyAccessor = keyAccessor;
        this.keyAllocator = keyAllocator;
        this.btreeAllocator = btreeAllocator;
        this.nodeSize = nodeSize;
        this.nodeSplitStrategy = nodeSplitStrategy;
    }

    /**
     * Allocates new off-heap node, write locks it and initializes its header.
     *
     * @param lockingContext the locking context
     * @return the write locked node's address
     */
    long newNodeLocked(LockingContext lockingContext) {
        long address = getBtreeAllocator().allocate(nodeSize);
        // Don't initialize lockState and sequence number fields to avoid race condition with lookup operations
        lockManager.writeLock(address);
        lockingContext.addLock(address);
        setNodeLevel(address, 0);
        setKeysCount(address, 0);
        return address;
    }

    /**
     * Disposes the node.
     *
     * @param nodeAddr the node address
     */
    void disposeNode(long nodeAddr) {
        if (nodeAddr != NULL_ADDRESS) {
            getBtreeAllocator().free(nodeAddr, nodeSize);
        }
    }

    abstract int getOffsetEntries();

    // Used for unit testing only
    void setNodeSplitStrategy(NodeSplitStrategy strategy) {
        this.nodeSplitStrategy = strategy;
    }

    static long getLockStateAddr(long nodeAddr) {
        return nodeAddr + OFFSET_LOCK_STATE;
    }

    static long getLockState(long nodeAddr) {
        return AMEM.getLong(getLockStateAddr(nodeAddr));
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    static int getNodeLevel(long nodeAddr) {
        return (int) AMEM.getByte(nodeAddr + OFFSET_LEVEL) & 0xFF;
    }

    static void setNodeLevel(long nodeAddr, int level) {
        AMEM.putByte(nodeAddr + OFFSET_LEVEL, (byte) level);
    }

    static boolean isInnerNode(long nodeAddr) {
        return getNodeLevel(nodeAddr) > 0;
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    static int getKeysCount(long nodeAddr) {
        return (int) AMEM.getShort(nodeAddr + OFFSET_KEYS_COUNT) & 0xFFFF;
    }

    static void setKeysCount(long nodeAddr, int count) {
        AMEM.putShort(nodeAddr + OFFSET_KEYS_COUNT, (short) count);
    }

    abstract boolean isNodeFull(long nodeAddr);

    static long getSequenceNumber(long nodeAddr) {
        return AMEM.getLong(nodeAddr + OFFSET_SEQUENCE_NUMBER);
    }

    static void setSequenceCounter(long nodeAddr, long value) {
        AMEM.putLong(nodeAddr + OFFSET_SEQUENCE_NUMBER, value);
    }

    static void incSequenceCounter(long nodeAddr) {
        long sequenceNumber = getSequenceNumber(nodeAddr);
        setSequenceCounter(nodeAddr, sequenceNumber + 1);
    }

    Data getIndexKeyHeapData(long nodeAddr, int slot) {
        long indexKeyAddr = getIndexKeyAddr(nodeAddr, slot);
        return keyAccessor.convertToHeapData(indexKeyAddr);
    }

    // used for unit testing only
    Data getIndexKeyHeapDataOrNull(long nodeAddr, int slot) {
        long indexKeyAddr = getIndexKeyAddr(nodeAddr, slot);
        if (indexKeyAddr == NULL_ADDRESS) {
            return null;
        }
        return keyAccessor.convertToHeapData(indexKeyAddr);
    }

    long getIndexKeyAddr(long nodeAddr, int slot) {
        return AMEM.getLong(nodeAddr + getOffsetEntries() + (long) slot * SLOT_ENTRY_SIZE);
    }

    void setIndexKey(long nodeAddr, int slot, long indexKeyAddr) {
        AMEM.putLong(nodeAddr + getOffsetEntries() + (long) slot * SLOT_ENTRY_SIZE, indexKeyAddr);
    }

    NativeMemoryData getEntryKey(long nodeAddr, int slot) {
        return new NativeMemoryData().reset(getEntryKeyAddr(nodeAddr, slot));
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    long getEntryKeyAddr(long nodeAddr, int slot) {
        return AMEM.getLong(nodeAddr + getOffsetEntries() + (long) slot * SLOT_ENTRY_SIZE + 8);
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    void setEntryKey(long nodeAddr, int slot, NativeMemoryData entryKey) {
        long entryKeyAddr = nodeAddr + getOffsetEntries() + (long) slot * SLOT_ENTRY_SIZE + 8;
        AMEM.putLong(entryKeyAddr, entryKey.address());
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    void setEntryKey(long nodeAddr, int slot, long entryKeyAddr) {
        AMEM.putLong(nodeAddr + getOffsetEntries() + (long) slot * SLOT_ENTRY_SIZE + 8, entryKeyAddr);
    }

    NativeMemoryData getValue(long nodeAddr, int slot) {
        return new NativeMemoryData().reset(getValueAddr(nodeAddr, slot));
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    long getValueAddr(long nodeAddr, int slot) {
        return AMEM.getLong(nodeAddr + getOffsetEntries() + (long) slot * SLOT_ENTRY_SIZE + 16);
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    void setValue(long nodeAddr, int slot, MemoryBlock value) {
        long valueAddr = nodeAddr + getOffsetEntries() + (long) slot * SLOT_ENTRY_SIZE + 16;
        AMEM.putLong(valueAddr, value.address());
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    void setValue(long nodeAddr, int slot, long valueAddr) {
        AMEM.putLong(nodeAddr + getOffsetEntries() + (long) slot * SLOT_ENTRY_SIZE + 16, valueAddr);
    }

    CompositeKeyComparison compareKeys(Comparable leftIndexKey, Data leftEntryKey, long nodeAddr, int slot) {
        return compareKeys0(leftIndexKey, leftEntryKey, nodeAddr, slot, false);
    }

    /**
     * Compares the provided left and right keys. Deserializes the index key component if needed.
     * Compares entry key component in the serialized form.
     *
     * @param leftIndexKey        the left index key
     * @param leftEntryKey        the left entry key
     * @param nodeAddr            the node address of the right-hand key
     * @param slot                the slot of the right-hand key in the node
     * @param cmpOnlyIndexKeyPart whether to compare only using index key component
     * @return INDEX_KEY_LESS if the left-hand index key is less than right-hand index key;
     * <p>
     * INDEX_KEY_GREATER if the left-hand index key is greater than right-hand index key;
     * <p>
     * KEYS_EQUAL if both the left-hand index key and entry key equal to the
     * right-hand index key and entry key. If the caller requested a comparison
     * of the index key component only (cmpOnlyIndexKeyPart = true), the entry key is not compared.
     * <p>
     * INDEX_KEY_EQUAL_ENTRY_KEY_LESS if the left-hand index key is equal to the right-hand index key and the
     * left-hand entry key is less than the right-hand entry-key;
     * <p>
     * INDEX_KEY_EQUAL_ENTRY_KEY_GREATER if the left-hand index key is equal to the right-hand index key and the
     * left-hand entry key is greater than the right-hand entry-key.
     */
    @SuppressWarnings("checkstyle:MagicNumber")
    CompositeKeyComparison compareKeys0(Comparable leftIndexKey, Data leftEntryKey, long nodeAddr, int slot,
                                       boolean cmpOnlyIndexKeyPart) {
        long rightIndexKeyAddr = getIndexKeyAddr(nodeAddr, slot);
        int cmp = keyComparator.compare(leftIndexKey, rightIndexKeyAddr);
        if (cmp == 0 && !cmpOnlyIndexKeyPart) {
            // Right-hand operand comes from the slot and cannot be +infinity
            if (leftEntryKey == PLUS_INFINITY_ENTRY_KEY) {
                return INDEX_KEY_EQUAL_ENTRY_KEY_GREATER;
            }
            long rightEntryKeyAddr = getEntryKeyAddr(nodeAddr, slot);
            if (rightEntryKeyAddr == NULL_ADDRESS) {
                return leftEntryKey == null ? KEYS_EQUAL : INDEX_KEY_EQUAL_ENTRY_KEY_GREATER;
            }
            if (leftEntryKey == null) {
                return INDEX_KEY_EQUAL_ENTRY_KEY_LESS;
            }

            NativeMemoryData rightEntryKey = getEntryKey(nodeAddr, slot);
            int cmp2 = keyComparator.compareSerializedEntryKeys(leftEntryKey, rightEntryKey);
            return cmp2 < 0 ? INDEX_KEY_EQUAL_ENTRY_KEY_LESS : cmp2 == 0 ? KEYS_EQUAL
                    : INDEX_KEY_EQUAL_ENTRY_KEY_GREATER;
        }
        return cmp < 0 ? INDEX_KEY_LESS : cmp == 0 ? KEYS_EQUAL : INDEX_KEY_GREATER;
    }

    long getSlotAddr(long nodeAddr, int slot) {
        return nodeAddr + getOffsetEntries() + (long) slot * SLOT_ENTRY_SIZE;
    }

    void copyNodeContent(long srcNodeAddr, long destNodeAddr) {
        long contentSize = nodeSize - OFFSET_NODE_CONTENT;
        AMEM.copyMemory(srcNodeAddr + OFFSET_NODE_CONTENT, destNodeAddr + OFFSET_NODE_CONTENT, contentSize);
    }

    MemoryAllocator getKeyAllocator() {
        return keyAllocator;
    }

    MemoryAllocator getBtreeAllocator() {
        return btreeAllocator;
    }
}
