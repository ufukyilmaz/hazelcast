package com.hazelcast.internal.bplustree;

import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.hazelcast.internal.bplustree.CompositeKeyComparison.greater;
import static com.hazelcast.internal.bplustree.CompositeKeyComparison.greaterOrEqual;
import static com.hazelcast.internal.bplustree.CompositeKeyComparison.less;
import static com.hazelcast.internal.bplustree.CompositeKeyComparison.lessOrEqual;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.MAX_LEVEL;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.MAX_NODE_SIZE;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.MIN_NODE_SIZE;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getKeysCount;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getLockStateAddr;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getNodeLevel;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getSequenceNumber;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.incSequenceCounter;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.isInnerNode;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.setKeysCount;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.setNodeLevel;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.internal.util.QuickMath.isPowerOfTwo;
import static com.hazelcast.internal.util.QuickMath.nextPowerOfTwo;

/**
 * Off-heap implementation of the concurrent B+tree data structure. All public methods implementing
 * the {@link BPlusTree} interface are thread-safe.
 * <p>
 * The high level overview of the B+tree can be found in https://en.wikipedia.org/wiki/B%2B_tree
 * <p>
 * Every node has a fixed size. There are 2 types of nodes: inner and leaf nodes.
 * <p>
 * Inner node always has links to the children nodes.
 * <p>
 * The basic structure of the B+tree node is as follows
 * <pre>
 * +--------------------------------------------+---------+
 * |             Slots area                     | Header  |
 * +--------------------------------------------+---------+
 *  </pre>
 * <p>
 * The inner node header has 4 fields:
 * <ul>
 *    <li>8 bytes, the lock state
 *    <li>8 bytes, the sequence number - monotonically incrementing value even when the node is released
 *    <li>1 byte, the node level; leaf node has 0 level, root node has the highest level
 *    <li>2 bytes, the keys count stored in the node.
 * </ul>
 * <p>
 * The leaf node header has 6 fields:
 * <ul>
 *    <li>8 bytes, the lock state.
 *    <li>8 bytes, the sequence number - monotonically incrementing value even when the node is released
 *    <li>1 byte, the node level; leaf node always has 0 level.
 *    <li>2 bytes, the keys count stored in the node.
 *    <li>8 bytes, the address of the forward leaf node, NULL_ADDRESS if it doesn't exist.
 *    <li>8 bytes, the address of the backward leaf node, NULL_ADDRESS if it doesn't exist.
 *    <li>
 * </ul>
 * <p>
 * The slot area contains B+tree index entries. The slot consists of 3 components:
 * <ul>
 *     <li>A 8 bytes off-heap address of the index key.
 *     <li>A 8 bytes off-heap address of the entry key.
 *     <li>A 8 bytes off-heap address of the index value (for the leaf node)
 *     or address of the child node (for the inner node).
 * </ul>
 * <p>
 * That is, every slot is fixed size and takes 24 bytes.
 * <p>
 * For a leaf node the slots count on the node always corresponds to the keys count stored in the header of the node.
 * <p>
 * For inner node the slots count on the nodes is always keysCount + 1. That is, if the keysCount = 0, the inner node
 * has one slot pointing to the child node. The index/entry key components are ignored in this case.
 * <p>
 * The leaf node "owns" index key component of the slot and on slot remove the index key is disposed.
 * <p>
 * The inner node "owns" both index key and entry key components and on slot remove they are both disposed.
 * <p>
 * To make the B+tree operations thread-safe a lock coupling approach is used, that is before accessing
 * the B+tree node, it should be locked in a shared or exclusive mode (depending on the operation) and the child
 * node should be locked before releasing a lock on the parent.
 * <p>
 * While the B+tree can increase the depth, the root node's address never changes.
 * <p>
 * A valid B+tree index always has at least 1 level, that is it always has at least one inner and one leaf node.
 * <p>
 * A batching of iterator results is supported to minimize the readLock/releaseLock calls count and
 * potential iterator re-synchronization.
 *
 * @param <T> the type of the lookup/range scan entries
 */
@SuppressWarnings("checkstyle:MethodCount")
public final class HDBPlusTree<T extends QueryableEntry> implements BPlusTree<T> {

    // The initial buffer size for a lookup batch
    static final int LOOKUP_INITIAL_BUFFER_SIZE = 8;

    // The +infinity value for the entry key. This value is greater than
    // any other entry key.
    static final Data PLUS_INFINITY_ENTRY_KEY = new NativeMemoryData();

    // The -infinity value for the entry key. This value is less than
    // any other entry key.
    static final Data MINUS_INFINITY_ENTRY_KEY = new NativeMemoryData();

    // The default maximum batch size for B+tree scan operation
    static final int DEFAULT_BPLUS_TREE_SCAN_BATCH_MAX_SIZE = 1000;

    // A special value for off-heap address to indicate that an attempt to
    // acquire a lock failedб and to avoid potential deadlock the caller has to
    // restart the B+tree operation. The value is an invalid off-heap address.
    private static final long RETRY_OPERATION = 0xFFFFFFFFFFFFFFFFL;

    // The Thread local to cache LockingContext object
    private static final ThreadLocal<LockingContext> LOCKING_CONTEXT =
            ThreadLocal.withInitial(() -> newLockingContext());

    private static final int NULL_SLOT = -1;

    // The factory to build lookup result entries. Must produce the entries on-heap
    private final MapEntryFactory<T> mapEntryFactory;

    // An utility accessor for the inner nodes
    private final HDBTreeInnerNodeAccessor innerNodeAccessor;

    // An utility accessor for the leaf nodes
    private final HDBTreeLeafNodeAccessor leafNodeAccessor;

    // The serialization service
    private final EnterpriseSerializationService ess;

    // The lock manager to handle locks on the B+tree nodes
    private final LockManager lockManager;

    private final Object clearMutex = new Object();

    // The address of the root node
    private final long rootAddr;

    // Indicates whether the B+tree is already disposed
    private volatile boolean isDisposed;

    // The maximum batch size for B+tree scan operation,
    // 0 value disables batching
    private final int btreeScanBatchSize;

    /**
     * Constructs new B+tree. Both the keyAllocator and the btreeAllocator must be thread-safe.
     *
     * @param ess                the serialization service
     * @param keyAllocator       the memory allocator for leaf and inner keys on the node
     * @param btreeAllocator     the memory allocator for B+tree node
     * @param lockManager        the lock manager
     * @param keyComparator      the off-heap keys comparator
     * @param keyAccessor        the index key component accessor
     * @param entryFactory       The factory to build lookup result entries. Must produce the entries on-heap
     * @param nodeSize           the B+tree node size
     * @param btreeScanBatchSize The maximum batch size for B+tree scan operation, 0 value disabled batching,
     *                           negative value is not allowed
     */
    private HDBPlusTree(EnterpriseSerializationService ess,
                        MemoryAllocator keyAllocator,
                        MemoryAllocator btreeAllocator,
                        LockManager lockManager,
                        BPlusTreeKeyComparator keyComparator,
                        BPlusTreeKeyAccessor keyAccessor,
                        MapEntryFactory<T> entryFactory,
                        int nodeSize,
                        int btreeScanBatchSize,
                        EntrySlotPayload entrySlotPayload) {
        if (nodeSize <= 0 || !isPowerOfTwo(nodeSize)) {
            throw new IllegalArgumentException("Illegal node size " + nodeSize
                    + ". Node size must be a power of two");
        }
        if (nodeSize < MIN_NODE_SIZE || nodeSize > MAX_NODE_SIZE) {
            throw new IllegalArgumentException("Illegal node size " + nodeSize
                    + ". Node size cannot exceed " + MAX_NODE_SIZE + " and be less than " + MIN_NODE_SIZE);
        }

        if (btreeScanBatchSize < 0) {
            throw new IllegalArgumentException("Range scan batch size " + btreeScanBatchSize + " must be positive or zero");
        }
        this.mapEntryFactory = entryFactory;
        this.ess = ess;
        this.lockManager = lockManager;
        this.btreeScanBatchSize = btreeScanBatchSize;
        NodeSplitStrategy splitStrategy = new DefaultNodeSplitStrategy();

        this.innerNodeAccessor = new HDBTreeInnerNodeAccessor(lockManager, ess, keyComparator,
                keyAccessor, keyAllocator, btreeAllocator, nodeSize, splitStrategy, entrySlotPayload);
        this.leafNodeAccessor = new HDBTreeLeafNodeAccessor(lockManager, ess, keyComparator,
                keyAccessor, keyAllocator, btreeAllocator, nodeSize, splitStrategy, entrySlotPayload);
        LockingContext lockingContext = getLockingContext();
        rootAddr = innerNodeAccessor.newNodeLocked(lockingContext);
        createEmptyTree(lockingContext);
        assert getNodeLevel(rootAddr) == 1;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public static <T extends Map.Entry> HDBPlusTree newHDBTree(EnterpriseSerializationService ess,
                                                               MemoryAllocator keyAllocator,
                                                               MemoryAllocator btreeAllocator,
                                                               BPlusTreeKeyComparator keyComparator,
                                                               BPlusTreeKeyAccessor keyAccessor,
                                                               MapEntryFactory<T> entryFactory,
                                                               int nodeSize,
                                                               EntrySlotPayload entrySlotPayload) {
        int stripesCount = nextPowerOfTwo(Runtime.getRuntime().availableProcessors() * 4);
        LockManager lockManager = new HDLockManager(stripesCount);
        return new HDBPlusTree(ess, keyAllocator, btreeAllocator, lockManager, keyComparator,
                keyAccessor, entryFactory, nodeSize, DEFAULT_BPLUS_TREE_SCAN_BATCH_MAX_SIZE, entrySlotPayload);
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    public static <T extends Map.Entry> HDBPlusTree newHDBTree(EnterpriseSerializationService ess,
                                                               MemoryAllocator keyAllocator,
                                                               MemoryAllocator btreeAllocator, LockManager lockManager,
                                                               BPlusTreeKeyComparator keyComparator,
                                                               BPlusTreeKeyAccessor keyAccessor,
                                                               MapEntryFactory<T> entryFactory,
                                                               int nodeSize,
                                                               int rangeScanBatchSize,
                                                               EntrySlotPayload entrySlotPayload) {
        return new HDBPlusTree(ess, keyAllocator, btreeAllocator, lockManager, keyComparator,
                keyAccessor, entryFactory, nodeSize, rangeScanBatchSize, entrySlotPayload);
    }

    private void createEmptyTree(LockingContext lockingContext) {
        try {
            incrementBTreeDepthRootLocked(lockingContext);
        } catch (Throwable e) {
            dispose0(lockingContext);
            releaseLocks(lockingContext);
            throw e;
        } finally {
            assert lockingContext.hasNoLocks();
        }
    }

    /**
     * Returns an address of the root node.
     */
    long getRoot() {
        return rootAddr;
    }

    /**
     * Increments a depth of the B+tree. The root node must be write locked by the caller.
     * <p>
     * Releases all locks in the end of the operation.
     *
     * @param lockingContext the locking context
     */
    private void incrementBTreeDepthRootLocked(LockingContext lockingContext) {
        // Create and lock the new node
        int rootLevel = getNodeLevel(rootAddr);
        if (rootLevel == MAX_LEVEL) {
            releaseLock(rootAddr, lockingContext);
            // We've reached the maximum level limit
            throw new BPlusTreeLimitException("Failed to increment BTree's level. Reached the maximum " + MAX_LEVEL);
        }
        long newChildAddr = rootLevel == 0 ? leafNodeAccessor.newNodeLocked(lockingContext)
                : innerNodeAccessor.newNodeLocked(lockingContext);
        if (rootLevel > 0) {
            // Copy content of the root node to the new node
            innerNodeAccessor.copyNodeContent(rootAddr, newChildAddr);
        }
        // Increment the root level
        setNodeLevel(rootAddr, rootLevel + 1);
        setKeysCount(rootAddr, 0);
        innerNodeAccessor.insert(rootAddr, newChildAddr);
        releaseLock(rootAddr, lockingContext);
        releaseLock(newChildAddr, lockingContext);
    }

    @Override
    public NativeMemoryData insert(Comparable indexKey, NativeMemoryData entryKey, MemoryBlock value) {
        LockingContext lockingContext = getLockingContext();
        try {
            Comparable indexKey0 = getKeyComparator().wrapIndexKey(indexKey);
            return insert(indexKey0, entryKey, value, lockingContext);
        } catch (Throwable e) {
            releaseLocks(lockingContext);
            throw e;
        } finally {
            assert lockingContext.hasNoLocks();
        }
    }

    private NativeMemoryData insert(Comparable indexKey, NativeMemoryData entryKey, MemoryBlock value,
                                    LockingContext lockingContext) {

        restart:
        for (; ; ) {
            readLock(rootAddr, lockingContext);

            long nodeAddr = rootAddr;
            long parentAddr = NULL_ADDRESS;

            // Traverse top -> down to the leaf
            while (isInnerNode(nodeAddr)) {
                parentAddr = nodeAddr;

                nodeAddr = innerNodeAccessor.getChildNode(nodeAddr, indexKey, entryKey);
                if (getNodeLevel(parentAddr) == 1) {
                    // Write lock leaf node for insert, keep read lock on the parent
                    writeLock(nodeAddr, lockingContext);
                    break;
                } else {
                    readLock(nodeAddr, lockingContext);
                }

                releaseLock(parentAddr, lockingContext);
            }

            // Now leaf node is write locked and parent read locked
            long leafAddr = nodeAddr;

            if (leafNodeAccessor.isNodeFull(leafAddr)) {
                // Split leaf node, if full

                if (!upgradeToWriteLock(parentAddr)) {
                    releaseLock(parentAddr, lockingContext);
                    releaseLock(leafAddr, lockingContext);
                    splitNode(indexKey, entryKey, lockingContext);
                    continue restart;
                } else {
                    // Try to split the leaf page w/o extra top -> down search
                    if (!splitLockedNodeWithParentLocked(parentAddr, leafAddr, lockingContext)) {
                        splitNode(indexKey, entryKey, lockingContext);
                    }
                    continue restart;
                }
            } else {
                // Leaf can accommodate one more slot
                releaseLock(parentAddr, lockingContext);
                NativeMemoryData oldValue = leafNodeAccessor.insert(leafAddr, indexKey, entryKey, value);
                releaseLock(leafAddr, lockingContext);
                return oldValue;
            }
        }
    }

    /**
     * Splits the leaf node identifiable by the indexKey/entryKey. There should be no locks on the
     * B+tree nodes acquired by the caller.
     * <p>
     * Releases all locks in the end of the operation.
     *
     * @param indexKey       the index key
     * @param entryKey       the entry key
     * @param lockingContext the locking context
     */
    private void splitNode(Comparable indexKey, NativeMemoryData entryKey, LockingContext lockingContext) {
        int splitLevel = 0;

        restart:
        for (; ; ) {
            assert splitLevel >= 0;
            readLock(rootAddr, lockingContext);
            long nodeAddr = rootAddr;
            int rootLevel = getNodeLevel(rootAddr);

            if (rootLevel == splitLevel) {
                // Root node needs a split, increase the tree depth first
                if (upgradeToWriteLock(rootAddr)) {
                    incrementBTreeDepthRootLocked(lockingContext);
                    continue restart;
                } else {
                    releaseLock(rootAddr, lockingContext);
                    writeLock(rootAddr, lockingContext);
                    rootLevel = getNodeLevel(rootAddr);
                    if (rootLevel == splitLevel) {
                        incrementBTreeDepthRootLocked(lockingContext);
                        continue restart;
                    }
                }
            } else if (rootLevel == splitLevel + 1) {
                // Make sure the parent is write locked
                if (!upgradeToWriteLock(nodeAddr)) {
                    releaseLock(nodeAddr, lockingContext);
                    writeLock(nodeAddr, lockingContext);
                }
            }

            for (; ; ) {
                long parentAddr = nodeAddr;

                nodeAddr = innerNodeAccessor.getChildNode(nodeAddr, indexKey, entryKey);
                int parentLevel = getNodeLevel(parentAddr);
                if (parentLevel == splitLevel + 1) {
                    writeLock(nodeAddr, lockingContext);
                    if (splitLockedNodeWithParentLocked(parentAddr, nodeAddr, lockingContext)) {
                        if (splitLevel == 0) {
                            // We've managed to make a split on the desired level
                            return;
                        } else {
                            splitLevel--;
                            assert splitLevel >= 0;
                            continue restart;
                        }
                    } else {
                        // Parent is full, split it
                        splitLevel++;
                        continue restart;
                    }
                } else if (parentLevel == splitLevel + 2) {
                    writeLock(nodeAddr, lockingContext);
                } else {
                    readLock(nodeAddr, lockingContext);
                }
                releaseLock(parentAddr, lockingContext);
            }
        }
    }

    /**
     * Splits the child. Assumes that the parent and child are write locked by the caller.
     * <p>
     * Releases all locks in the end of the operation.
     *
     * @param parentAddr     the parent address
     * @param childAddr      the child address
     * @param lockingContext the locking context
     * @return {@code true} if the split was successful and the child has space for the new slot, {@code false} otherwise
     * (parent split is needed)
     */
    private boolean splitLockedNodeWithParentLocked(long parentAddr, long childAddr, LockingContext lockingContext) {
        if (getNodeLevel(childAddr) == 0) {
            // Split leaf node
            return splitLockedLeafWithParentLocked(parentAddr, childAddr, lockingContext);
        } else {
            // Split inner node
            return splitLockedInnerWithParentLocked(parentAddr, childAddr, lockingContext);
        }
    }

    /**
     * Splits the leaf. The leaf and its parent must be write locked by the caller.
     * <p>
     * Releases all locks in the end of the operation.
     *
     * @param parentAddr     the parent address
     * @param childAddr      the child (leaf) address
     * @param lockingContext the locking context
     * @return {@code true} if the split was successful and the leaf has space for the new slot, {@code false} otherwise
     * (parent split is needed)
     */
    private boolean splitLockedLeafWithParentLocked(long parentAddr, long childAddr, LockingContext lockingContext) {
        assert getNodeLevel(childAddr) == 0;

        if (!leafNodeAccessor.isNodeFull(childAddr)) {
            // Leaf is not full, most likely it was splitted by concurrent operation
            releaseLock(parentAddr, lockingContext);
            releaseLock(childAddr, lockingContext);
            return true;
        }
        // No space in the parent, need to split it as well
        if (innerNodeAccessor.isNodeFull(parentAddr)) {
            releaseLock(parentAddr, lockingContext);
            releaseLock(childAddr, lockingContext);
            return false;
        }

        long forwAddr = leafNodeAccessor.getForwNode(childAddr);
        if (forwAddr != NULL_ADDRESS) {
            writeLock(forwAddr, lockingContext);
        }

        // Split the leaf node, new leaf is write locked
        int sepSlot = leafNodeAccessor.getSeparationSlot(childAddr);
        long sepIndexKeyAddr = leafNodeAccessor.getIndexKeyAddr(childAddr, sepSlot);
        long sepEntryKeyAddr = leafNodeAccessor.getEntryKeyAddr(childAddr, sepSlot);

        long clonedSepIndexKeyAddr = NULL_ADDRESS;
        long clonedSepEntryKeyAddr = NULL_ADDRESS;
        long newLeafAddr = NULL_ADDRESS;

        try {
            clonedSepIndexKeyAddr = innerNodeAccessor.clonedIndexKeyAddr(sepIndexKeyAddr);
            clonedSepEntryKeyAddr = innerNodeAccessor.clonedEntryKeyAddr(sepEntryKeyAddr);

            newLeafAddr = leafNodeAccessor.split(childAddr, lockingContext);
        } catch (NativeOutOfMemoryError oom) {
            try {
                innerNodeAccessor.disposeAddresses(clonedSepIndexKeyAddr, clonedSepEntryKeyAddr);
            } finally {
                leafNodeAccessor.disposeNode(newLeafAddr);
            }
            throw oom;
        }

        // Update back/forw links
        leafNodeAccessor.setForwNode(newLeafAddr, forwAddr);
        leafNodeAccessor.setBackNode(newLeafAddr, childAddr);
        if (forwAddr != NULL_ADDRESS) {
            leafNodeAccessor.setBackNode(forwAddr, newLeafAddr);
            incSequenceCounter(forwAddr);
        }
        leafNodeAccessor.setForwNode(childAddr, newLeafAddr);

        // Insert the split key into the parent
        innerNodeAccessor.insert(parentAddr, clonedSepIndexKeyAddr, clonedSepEntryKeyAddr, newLeafAddr);

        releaseLock(parentAddr, lockingContext);
        releaseLock(childAddr, lockingContext);
        releaseLock(newLeafAddr, lockingContext);
        if (forwAddr != NULL_ADDRESS) {
            releaseLock(forwAddr, lockingContext);
        }
        return true;
    }

    /**
     * Splits the inner node. The inner node and its parent must be write locked.
     * <p>
     * Releases all locks in the end of the operation.
     *
     * @param parentAddr     the parent address
     * @param childAddr      the child address (the node to be splitted)
     * @param lockingContext the locking context
     * @return {@code true} if the split was successful and the inner node has space for the new slot,
     * {@code false} otherwise (parent split is needed)
     */
    private boolean splitLockedInnerWithParentLocked(long parentAddr, long childAddr, LockingContext lockingContext) {
        assert getNodeLevel(childAddr) > 0;

        if (!innerNodeAccessor.isNodeFull(childAddr)) {
            // Inner node is not full, apparently it was splitted by concurrent operation
            releaseLock(parentAddr, lockingContext);
            releaseLock(childAddr, lockingContext);
            return true;
        }

        // No space in the parent, need to split it as well
        if (innerNodeAccessor.isNodeFull(parentAddr)) {
            releaseLock(parentAddr, lockingContext);
            releaseLock(childAddr, lockingContext);
            return false;
        }

        // Split inner node, new inner node write locked
        long newInnerAddr = innerNodeAccessor.split(childAddr, lockingContext);

        // Insert the split key into the parent
        // Don't clone the separator key, because it is "borrowed" from the child node
        int sepSlot = getKeysCount(childAddr);
        long sepIndexKeyAddr = innerNodeAccessor.getIndexKeyAddr(childAddr, sepSlot);
        long sepEntryKeyAddr = innerNodeAccessor.getEntryKeyAddr(childAddr, sepSlot);
        innerNodeAccessor.insert(parentAddr, sepIndexKeyAddr, sepEntryKeyAddr, newInnerAddr);

        releaseLock(parentAddr, lockingContext);
        releaseLock(childAddr, lockingContext);
        releaseLock(newInnerAddr, lockingContext);
        return true;
    }

    @Override
    public NativeMemoryData remove(Comparable indexKey, NativeMemoryData entryKey) {
        LockingContext lockingContext = getLockingContext();
        try {
            Comparable indexKey0 = getKeyComparator().wrapIndexKey(indexKey);
            return remove(indexKey0, entryKey, lockingContext);
        } catch (Throwable e) {
            releaseLocks(lockingContext);
            throw e;
        } finally {
            assert lockingContext.hasNoLocks();
        }
    }

    private NativeMemoryData remove(Comparable indexKey, NativeMemoryData entryKey, LockingContext lockingContext) {
        readLock(rootAddr, lockingContext);

        long nodeAddr = rootAddr;
        long parentAddr;

        // Traverse top -> down to the leaf
        while (isInnerNode(nodeAddr)) {
            parentAddr = nodeAddr;

            nodeAddr = innerNodeAccessor.getChildNode(nodeAddr, indexKey, entryKey);
            if (getNodeLevel(parentAddr) == 1) {
                // Write lock leaf node for remove
                writeLock(nodeAddr, lockingContext);
            } else {
                readLock(nodeAddr, lockingContext);
            }

            releaseLock(parentAddr, lockingContext);
        }

        // Now, the leaf node is write locked
        boolean emptyLeaf = false;
        NativeMemoryData oldValue = leafNodeAccessor.remove(nodeAddr, indexKey, entryKey);
        if (getKeysCount(nodeAddr) == 0) {
            emptyLeaf = true;
        }
        releaseLock(nodeAddr, lockingContext);
        if (emptyLeaf) {
            removeNodeFromBTree(indexKey, entryKey, lockingContext);
        }
        return oldValue;
    }

    /**
     * Removes a sub-tree identifiable by the indexKey/entryKey. In majority of cases
     * the sub-tree is an individual leaf node.
     * <p>
     * There should be no locks on the B+tree nodes acquired by the caller.
     * <p>
     * Releases all locks in the end of the operation.
     *
     * @param indexKey       the index key
     * @param entryKey       the entry key
     * @param lockingContext the locking context
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NestedIfDepth"})
    private void removeNodeFromBTree(Comparable indexKey, NativeMemoryData entryKey, LockingContext lockingContext) {

        restart:
        for (; ; ) {
            readLock(rootAddr, lockingContext);

            long nodeAddr = rootAddr;
            int rootLevel = getNodeLevel(rootAddr);
            assert rootLevel >= 1;
            List<Long> innerNodes = Collections.emptyList();

            if (rootLevel == 1) {
                // Root node is a parent of the node to be removed
                if (upgradeToWriteLock(rootAddr)) {
                    if (removeNodeFromAncestorWriteLocked(indexKey, entryKey, rootAddr, rootAddr, innerNodes, lockingContext)) {
                        return;
                    }
                    continue restart;
                } else {
                    releaseLock(rootAddr, lockingContext);
                    writeLock(rootAddr, lockingContext);
                    rootLevel = getNodeLevel(rootAddr);
                    if (rootLevel == 1) {
                        if (removeNodeFromAncestorWriteLocked(indexKey, entryKey,
                                rootAddr, rootAddr, innerNodes, lockingContext)) {
                            return;
                        }
                        continue restart;
                    } else {
                        releaseLock(rootAddr, lockingContext);
                        continue restart;
                    }
                }
            }

            long ancestorAddr = nodeAddr;

            // Go top -> down collecting the write locked nodes of the sub-tree to be removed.
            // ancestorAddr defines an ancestor node where a link to the sub-tree will be removed.
            while (getNodeLevel(nodeAddr) > 1) {
                nodeAddr = innerNodeAccessor.getChildNode(nodeAddr, indexKey, entryKey);
                readLock(nodeAddr, lockingContext);

                if (getKeysCount(nodeAddr) == 0) {
                    if (innerNodes.isEmpty()) {
                        innerNodes = new ArrayList<>();
                    }
                    innerNodes.add(nodeAddr);
                } else {
                    releaseLock(ancestorAddr, lockingContext);
                    releaseLocks(innerNodes, lockingContext);
                    innerNodes = Collections.emptyList();
                    ancestorAddr = nodeAddr;
                }
            }

            if (!upgradeToWriteLock(ancestorAddr)
                    || !upgradeToWriteLock(innerNodes)) {
                releaseLock(ancestorAddr, lockingContext);
                releaseLocks(innerNodes, lockingContext);
                // To avoid livelock, instantly acquire a write lock on ancestor
                instantDurationWriteLock(ancestorAddr);
                continue restart;
            }

            if (removeNodeFromAncestorWriteLocked(indexKey, entryKey, nodeAddr, ancestorAddr, innerNodes, lockingContext)) {
                return;
            } else {
                continue restart;
            }
        }
    }

    /**
     * Removes a subtree identifiable by the indexKey/entryKey from the ancestor.
     * <p>
     * The path of nodes from ancestor (including) up to the leaf node (excluding) must be write locked
     * by the caller.
     * <p>
     * Releases all locks in the end of the operation.
     *
     * @param indexKey       the index key
     * @param entryKey       the entry key
     * @param parentAddr     the parent address of the leaf node to be removed
     * @param ancestorAddr   the ancestor address
     * @param innerNodes     the inner write locked nodes below ancestor
     * @param lockingContext the locking context
     * @return {@code true} if the sub-tree has been removed or no need for removal anymore, {@code false} otherwise and
     * the operation has to be repeated.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private boolean removeNodeFromAncestorWriteLocked(Comparable indexKey, NativeMemoryData entryKey, long parentAddr,
                                                      long ancestorAddr, List<Long> innerNodes, LockingContext lockingContext) {

        long childAddr = innerNodeAccessor.getChildNode(parentAddr, indexKey, entryKey);
        writeLock(childAddr, lockingContext);

        assert getNodeLevel(childAddr) == 0;
        long leftChildAddr = NULL_ADDRESS;
        long rightChildAddr = NULL_ADDRESS;
        boolean disposeNodes = false;

        assert getKeysCount(ancestorAddr) != 0 || ancestorAddr == rootAddr;

        // Continue removing only if the leaf node is still empty and the ancestor has the split key
        if (getKeysCount(childAddr) == 0 && getKeysCount(ancestorAddr) != 0) {

            // Try to lock the left child
            leftChildAddr = leafNodeAccessor.getBackNode(childAddr);

            if (leftChildAddr != NULL_ADDRESS && !tryWriteLock(leftChildAddr, lockingContext)) {
                assert getNodeLevel(leftChildAddr) == 0;
                // Cannot acquire a lock on the left node, blocking may cause a deadlock.
                // Release the locks and do instant locking on the left node to avoid livelock problem.
                releaseLock(ancestorAddr, lockingContext);
                releaseLocks(innerNodes, lockingContext);
                releaseLock(childAddr, lockingContext);
                instantDurationWriteLock(leftChildAddr);
                return false;
            }

            // Lock the right child
            rightChildAddr = leafNodeAccessor.getForwNode(childAddr);
            if (rightChildAddr != NULL_ADDRESS) {
                writeLock(rightChildAddr, lockingContext);
                assert getNodeLevel(rightChildAddr) == 0;
            }

            // Assert the child leaf is still part of the BTree
            assert leftChildAddr == NULL_ADDRESS || leafNodeAccessor.getForwNode(leftChildAddr) == childAddr;
            assert rightChildAddr == NULL_ADDRESS || leafNodeAccessor.getBackNode(rightChildAddr) == childAddr;

            // Remove the node from the ancestor
            innerNodeAccessor.remove(ancestorAddr, indexKey, entryKey);

            // Detach the node from the left neighbor
            if (leftChildAddr != NULL_ADDRESS) {
                leafNodeAccessor.setForwNode(leftChildAddr, rightChildAddr);
                incSequenceCounter(leftChildAddr);
            }

            // Detach the node from the right neighbor
            if (rightChildAddr != NULL_ADDRESS) {
                leafNodeAccessor.setBackNode(rightChildAddr, leftChildAddr);
                incSequenceCounter(rightChildAddr);
            }

            // Dispose the nodes
            disposeNodes = true;
        }

        releaseLock(ancestorAddr, lockingContext);
        releaseLocks(innerNodes, lockingContext);
        releaseLock(childAddr, lockingContext);
        if (leftChildAddr != NULL_ADDRESS) {
            releaseLock(leftChildAddr, lockingContext);
        }
        if (rightChildAddr != NULL_ADDRESS) {
            releaseLock(rightChildAddr, lockingContext);
        }

        if (disposeNodes) {
            innerNodes.forEach(nodeAddr -> innerNodeAccessor.disposeNode(nodeAddr));
            leafNodeAccessor.disposeNode(childAddr);
        }

        return true;
    }

    @Override
    public Iterator<T> lookup(Comparable indexKey) {
        if (indexKey == null) {
            throw new IllegalArgumentException("Index key cannot be null");
        }

        LockingContext lockingContext = getLockingContext();
        try {
            Comparable indexKey0 = getKeyComparator().wrapIndexKey(indexKey);
            Data fromEntryKey = MINUS_INFINITY_ENTRY_KEY;

            EntryIterator entryIterator = lookup0(indexKey0, fromEntryKey, true,
                indexKey0, true, false, false, lockingContext);
            return batchIterator(entryIterator, LOOKUP_INITIAL_BUFFER_SIZE);
        } catch (Throwable e) {
            releaseLocks(lockingContext);
            throw e;
        } finally {
            assert lockingContext.hasNoLocks();
        }
    }

    @Override
    public Iterator<T> lookup(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive, boolean descending) {
        LockingContext lockingContext = getLockingContext();
        try {
            EntryIterator entryIterator;
            if (from == null) {
                entryIterator = getEntries0(to, toInclusive, descending, lockingContext);
            } else {
                Comparable from0 = getKeyComparator().wrapIndexKey(from);
                Comparable to0 = getKeyComparator().wrapIndexKey(to);
                Data fromEntryKey;
                if (descending) {
                    fromEntryKey = fromInclusive ? PLUS_INFINITY_ENTRY_KEY : MINUS_INFINITY_ENTRY_KEY;
                } else {
                    fromEntryKey = fromInclusive ? MINUS_INFINITY_ENTRY_KEY : PLUS_INFINITY_ENTRY_KEY;
                }
                entryIterator = lookup0(from0, fromEntryKey, fromInclusive,
                    to0, toInclusive, descending, false, lockingContext);
            }

            return batchIterator(entryIterator, btreeScanBatchSize);
        } catch (Throwable e) {
            releaseLocks(lockingContext);
            throw e;
        } finally {
            assert lockingContext.hasNoLocks();
        }
    }

    private Iterator<T> batchIterator(EntryIterator it, int batchInitialSize) {
        if (btreeScanBatchSize > 0) {
            EntryBatch entryBatch = new EntryBatch(batchInitialSize, btreeScanBatchSize);
            if (it.hasNext()) {
                T next = it.next();
                entryBatch.add(next);
            }
            return new BatchingEntryIterator(it, entryBatch);
        } else {
            return it;
        }
    }

    @Override
    public Iterator<Data> keys() {
        LockingContext lockingContext = getLockingContext();
        try {
            EntryIterator entryIterator = getEntries0(false, lockingContext);
            return new KeyIterator(entryIterator);
        } catch (Throwable e) {
            releaseLocks(lockingContext);
            throw e;
        } finally {
            assert lockingContext.hasNoLocks();
        }
    }

    /**
     * Performs a range scan in the B+tree. If the iterator is re-syncing, the method
     * doesn't release a lock on the leaf storing the first element of the range.
     *
     * @param from           the beginning of the range
     * @param fromEntryKey   the entryKy component of the beginning of the range
     * @param fromInclusive  {@code true} if the beginning of the range is inclusive,
     *                       {@code false} otherwise.
     *                       If fromInclusive is {@code false}, the fromEntryKey must be non-null.
     * @param to             the end of the range
     * @param toInclusive    {@code true} if the end of the range is inclusive,
     *                       {@code false} otherwise.
     * @param descending     {@code true} if return entries iterator in an descending order,
     *                       {@code false} if return entries iterator in an ascending order.
     * @param resync         {@code true} if the lookup is used to re-synchronize the existing iterator,
     *                       {@code false} otherwise.
     * @param lockingContext the locking context
     * @return the iterator of the range scan
     */
    @SuppressWarnings({"checkstyle:NPathComplexity", "checkstyle:CyclomaticComplexity", "checkstyle:MethodLength"})
    private EntryIterator lookup0(Comparable from, Data fromEntryKey, boolean fromInclusive, Comparable to,
                                  boolean toInclusive, boolean descending, boolean resync,
                                  LockingContext lockingContext) {
        assert fromEntryKey != null || fromInclusive;

        restart:
        for (; ; ) {
            long nodeAddr = rootAddr;
            long parentAddr;

            readLock(rootAddr, lockingContext);

            // Go top -> down, find the leaf page containing 'from' key
            while (isInnerNode(nodeAddr)) {
                parentAddr = nodeAddr;
                nodeAddr = innerNodeAccessor.getChildNode(nodeAddr, from, fromEntryKey);
                readLock(nodeAddr, lockingContext);
                releaseLock(parentAddr, lockingContext);
            }

            EntryIterator it = new EntryIterator(to, toInclusive, descending);

            // Skip empty nodes
            nodeAddr = skipToNonEmptyNodeLocked(nodeAddr, descending, lockingContext);
            if (nodeAddr == RETRY_OPERATION) {
                continue restart;
            }
            if (nodeAddr == NULL_ADDRESS) {
                it.setNextEntryNull();
                return it;
            }

            // Now, nodeAddr is read locked
            int keysCount = getKeysCount(nodeAddr);
            int lower = 0;
            int upper = keysCount - 1;
            int slot;
            boolean skipCurrentSlot = false;

            // Do binary search in the leaf node to find 'from' key
            for (; ; ) {
                if (upper < lower) {
                    if (descending) {
                        if (upper == -1) {
                            // All keys in the node are greater than the search fromKey
                            slot = lower;
                            skipCurrentSlot = true;
                        } else {
                            slot = upper;
                        }
                    } else {
                        if (lower == keysCount) {
                            // All keys in the node are less than the search fromKey
                            slot = upper;
                            skipCurrentSlot = true;
                        } else {
                            slot = lower;
                        }
                    }

                    break;
                }
                int mid = (upper + lower) / 2;

                CompositeKeyComparison cmp = leafNodeAccessor.compareKeys(from, fromEntryKey, nodeAddr, mid);

                if (less(cmp)) {
                    upper = mid - 1;
                } else if (greater(cmp)) {
                    lower = mid + 1;
                } else {
                    slot = mid;
                    skipCurrentSlot = !fromInclusive;
                    break;
                }
            }

            assert slot < keysCount;
            it.nextSlot = slot;
            it.sequenceNumber = getSequenceNumber(nodeAddr);
            it.currentNodeAddr = nodeAddr;

            // Check the current slot is within the range boundaries
            boolean nextSlotWithinRange = it.nextSlotIsWithinRange();

            // Skip current slot if necessary
            if (nextSlotWithinRange && skipCurrentSlot) {
                // Consume current slot
                it.next();
                boolean lastSlot = descending ? slot == 0 : slot == keysCount - 1;
                if (!lastSlot) {
                    // Navigate to the next slot
                    it.nextSlot = descending ? slot - 1 : slot + 1;
                } else {
                    // Next key is on the next node, skip empty ones
                    nodeAddr = skipToNextNonEmptyNode(it.currentNodeAddr, descending, lockingContext);
                    if (nodeAddr == RETRY_OPERATION) {
                        continue restart;
                    } else if (nodeAddr == NULL_ADDRESS) {
                        // No keys in range anymore
                        it.setNextEntryNull();
                        return it;
                    }
                    it.sequenceNumber = getSequenceNumber(nodeAddr);
                    it.currentNodeAddr = nodeAddr;
                    it.nextSlot = descending ? getKeysCount(nodeAddr) - 1 : 0;
                }
                // Check the slot is still within the range boundaries
                it.nextSlotIsWithinRange();
            }

            // Release the lock if one of the following is true:
            // - the lookup is not part of iterator re-synchronization logic;
            // - the iterator has no more results.
            if (!resync || it.nextSlot == NULL_SLOT) {
                releaseLock(nodeAddr, lockingContext);
            }

            return it;
        }
    }

    /**
     * Performs a range scan in the B+tree starting from left-most or right-most entry.
     *
     * @param to             the end of the range
     * @param toInclusive    {@code true} if the end of the range is inclusive,
     *                       {@code false} otherwise.
     * @param descending     {@code true} if return entries iterator in an descending order,
     *                       {@code false} if return entries iterator in an ascending order.
     * @param lockingContext the locking context
     * @return an iterator of the range scan
     */
    private EntryIterator getEntries0(Comparable to, boolean toInclusive, boolean descending, LockingContext lockingContext) {
        restart:
        for (; ; ) {
            long nodeAddr = rootAddr;
            long parentAddr;

            readLock(rootAddr, lockingContext);

            // Go top -> down, find the leaf page containing 'from' key
            while (isInnerNode(nodeAddr)) {
                parentAddr = nodeAddr;
                nodeAddr = innerNodeAccessor.getValueAddr(nodeAddr, descending ? getKeysCount(nodeAddr) : 0);
                readLock(nodeAddr, lockingContext);
                releaseLock(parentAddr, lockingContext);
            }

            EntryIterator it = new EntryIterator(to, toInclusive, descending);

            // Skip empty nodes
            nodeAddr = skipToNonEmptyNodeLocked(nodeAddr, descending, lockingContext);
            if (nodeAddr == RETRY_OPERATION) {
                continue restart;
            }
            if (nodeAddr == NULL_ADDRESS) {
                it.setNextEntryNull();
                return it;
            }

            // Now, nodeAddr is read locked
            int keysCount = getKeysCount(nodeAddr);

            it.nextSlot = descending ? keysCount - 1 : 0;
            it.sequenceNumber = getSequenceNumber(nodeAddr);
            it.currentNodeAddr = nodeAddr;

            releaseLock(nodeAddr, lockingContext);
            it.nextSlotIsWithinRange();

            return it;
        }
    }

    private EntryIterator getEntries0(boolean descending, LockingContext lockingContext) {
        return getEntries0(null, true, descending, lockingContext);
    }

    private long skipToNextNonEmptyNode(long nodeAddr, boolean descending, LockingContext lockingContext) {
        long nextAddr = descending ? leafNodeAccessor.getBackNode(nodeAddr) : leafNodeAccessor.getForwNode(nodeAddr);
        if (nextAddr == NULL_ADDRESS) {
            releaseLock(nodeAddr, lockingContext);
            return NULL_ADDRESS;
        }

        if (descending) {
            if (!tryReadLock(nextAddr, lockingContext)) {
                releaseLock(nodeAddr, lockingContext);
                instantDurationReadLock(nextAddr);
                return RETRY_OPERATION;
            }
            releaseLock(nodeAddr, lockingContext);
            return skipToNonEmptyNodeLocked(nextAddr, descending, lockingContext);
        } else {
            readLock(nextAddr, lockingContext);
            releaseLock(nodeAddr, lockingContext);
            return skipToNonEmptyNodeLocked(nextAddr, descending, lockingContext);
        }
    }

    /**
     * Skips to the next non-empty node if the initial nodeAddr is empty.
     * It is assumed that the initial nodeAddr is read locked.
     * <p>
     * The returned nodeAddr is also read locked.
     *
     * @param nodeAddr   the starting node to check emptiness
     * @param descending {@code true} if navigate backward along the leaf nodes.
     * @return next read locked non-empty node, ot NULL_ADDRESS is it doesn't exist
     */
    private long skipToNonEmptyNodeLocked(long nodeAddr, boolean descending, LockingContext lockingContext) {
        if (nodeAddr == NULL_ADDRESS) {
            return NULL_ADDRESS;
        }

        long currentNodeAddr = nodeAddr;
        while (getKeysCount(currentNodeAddr) == 0) {
            if (descending) {
                long backAddr = leafNodeAccessor.getBackNode(currentNodeAddr);
                if (backAddr == NULL_ADDRESS) {
                    releaseLock(currentNodeAddr, lockingContext);
                    return NULL_ADDRESS;
                }

                if (tryReadLock(backAddr, lockingContext)) {
                    releaseLock(currentNodeAddr, lockingContext);
                    currentNodeAddr = backAddr;
                } else {
                    releaseLock(currentNodeAddr, lockingContext);
                    instantDurationReadLock(backAddr);
                    return RETRY_OPERATION;
                }
            } else {
                long forwAddr = leafNodeAccessor.getForwNode(currentNodeAddr);
                if (forwAddr == NULL_ADDRESS) {
                    releaseLock(currentNodeAddr, lockingContext);
                    return NULL_ADDRESS;
                }

                readLock(forwAddr, lockingContext);
                releaseLock(currentNodeAddr, lockingContext);
                currentNodeAddr = forwAddr;
            }
        }
        return currentNodeAddr;
    }

    private class EntryIterator implements Iterator<T> {

        final Comparable to;
        final boolean toInclusive;
        final boolean descending;

        int lastSlot = NULL_SLOT;
        int nextSlot = NULL_SLOT;
        // The next entry stored on-heap
        T nextEntry;
        // The last entry stored on-heap
        T lastEntry;
        // The next index stored on-heap
        Data nextIndexKey;
        // The last index key stored on-heap
        Data lastIndexKey;

        long currentNodeAddr;
        // The last sequence number read from the current node
        long sequenceNumber;

        EntryIterator(Comparable to, boolean toInclusive, boolean descending) {
            this.to = to;
            this.toInclusive = toInclusive;
            this.descending = descending;
        }

        @Override
        public boolean hasNext() {
            if (nextSlot != NULL_SLOT) {
                return true;
            }
            nextEntry();
            return nextSlot != NULL_SLOT;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            lastSlot = nextSlot;
            lastEntry = nextEntry;
            lastIndexKey = nextIndexKey;
            nextSlot = NULL_SLOT;

            T lastEntry = nextEntry;
            nextEntry = null;
            nextIndexKey = null;
            return lastEntry;
        }

        void nextEntry() {
            LockingContext lockingContext = getLockingContext();
            try {
                nextBatch0(null, lockingContext);
            } catch (Throwable e) {
                releaseLocks(lockingContext);
                throw e;
            } finally {
                assert lockingContext.hasNoLocks();
            }
        }

        /**
         * Collects the next batch of results from the iterator.
         *
         * @param entryBatch     the batch to collect entry results, {@code null} if batching is disabled
         * @param lockingContext the locking context
         */
        @SuppressWarnings({"checkstyle:NPathComplexity", "checkstyle:CyclomaticComplexity", "checkstyle:MethodLength",
                "checkstyle:NestedIfDepth"})
        void nextBatch0(EntryBatch entryBatch, LockingContext lockingContext) {

            assert nextSlot == NULL_SLOT;
            assert entryBatch == null || entryBatch.hasCapacity();
            if (lastSlot == NULL_SLOT) {
                // The iterator has reached the end
                return;
            }
            restart:
            for (; ; ) {
                readLock(currentNodeAddr, lockingContext);

                // Check whether the currentNode has changed, and if so the iterator requires a re-sync
                if (sequenceNumber != getSequenceNumber(currentNodeAddr)) {
                    /* The node has changed since the previous iteration. Find the current key again. */
                    releaseLock(currentNodeAddr, lockingContext);

                    Data lastEntryKeyData = lastEntry.getKeyData();
                    Comparable lastIndexKey = ess.toObject(this.lastIndexKey);
                    Comparable wrappedLastIndexKey = getKeyComparator().wrapIndexKey(lastIndexKey);

                    EntryIterator resyncedIt = lookup0(wrappedLastIndexKey, lastEntryKeyData, false, to, toInclusive,
                            descending, true, lockingContext);

                    if (resyncedIt.nextSlot == NULL_SLOT) {
                        // Key has been removed and we've reached the end
                        setNextEntryNull();
                        return;
                    }
                    sequenceNumber = resyncedIt.sequenceNumber;
                    currentNodeAddr = resyncedIt.currentNodeAddr;
                    nextSlot = resyncedIt.nextSlot;
                    nextEntry = resyncedIt.nextEntry;
                    nextIndexKey = resyncedIt.nextIndexKey;
                    if (entryBatch == null) {
                        // Batching is disabled, release a lock on the leaf
                        releaseLock(currentNodeAddr, lockingContext);
                        return;
                    } else {
                        // Batching is enabled, consume the element and
                        // proceed to fill in the batch with the next elements
                        // Don't release a lock on the leaf.
                        next();
                        entryBatch.add(lastEntry);
                    }
                }

                boolean nextSlotWithinRange;
                do {
                    // currentNode is read locked
                    assert lastSlot < getKeysCount(currentNodeAddr) && lastSlot >= 0;
                    if (descending) {
                        if (lastSlot == 0) {
                            /* Current node is exhausted. Try next node and skip it if it is empty */
                            do {
                                long backAddr = leafNodeAccessor.getBackNode(currentNodeAddr);
                                if (backAddr == NULL_ADDRESS) {
                                    setNextEntryNull();
                                    releaseLock(currentNodeAddr, lockingContext);
                                    return;
                                }

                                if (!tryReadLock(backAddr, lockingContext)) {
                                    releaseLock(currentNodeAddr, lockingContext);
                                    // If there is something in the batch, return it to the caller
                                    if (entryBatch != null && entryBatch.size() > 0) {
                                        return;
                                    } else {
                                        // Otherwise, restart the operation
                                        instantDurationReadLock(backAddr);
                                        continue restart;
                                    }
                                }
                                releaseLock(currentNodeAddr, lockingContext);
                                currentNodeAddr = backAddr;
                                sequenceNumber = getSequenceNumber(currentNodeAddr);
                                nextSlot = getKeysCount(currentNodeAddr) - 1;
                            } while (getKeysCount(currentNodeAddr) == 0);
                        } else {
                            nextSlot = lastSlot - 1;
                        }
                    } else {
                        if (lastSlot == getKeysCount(currentNodeAddr) - 1) {
                            /* Current node is exhausted. Try next node and skip it if it is empty */
                            do {
                                long forwAddr = leafNodeAccessor.getForwNode(currentNodeAddr);
                                if (forwAddr == NULL_ADDRESS) {
                                    setNextEntryNull();
                                    releaseLock(currentNodeAddr, lockingContext);
                                    return;
                                }

                                readLock(forwAddr, lockingContext);
                                releaseLock(currentNodeAddr, lockingContext);
                                currentNodeAddr = forwAddr;
                                sequenceNumber = getSequenceNumber(currentNodeAddr);
                                nextSlot = 0;
                            } while (getKeysCount(currentNodeAddr) == 0);
                        } else {
                            nextSlot = lastSlot + 1;
                        }
                    }

                    nextSlotWithinRange = nextSlotIsWithinRange();
                    if (entryBatch != null && nextSlotWithinRange) {
                        // Consume next element if batching is enabled
                        next();
                        entryBatch.add(lastEntry);
                    }

                } while (entryBatch != null && entryBatch.hasCapacity() && nextSlotWithinRange);

                releaseLock(currentNodeAddr, lockingContext);
                return;
            }
        }

        @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:BooleanExpressionComplexity"})
        boolean nextSlotIsWithinRange() {
            if (to == null) {
                setNextEntry();
                // Nothing to check
                return true;
            }

            if (nextSlot != NULL_SLOT) {
                CompositeKeyComparison cmp = leafNodeAccessor.compareKeys0(to, null, currentNodeAddr, nextSlot, true);
                if (descending && (toInclusive && lessOrEqual(cmp) || !toInclusive && less(cmp))
                        || !descending && (toInclusive && greaterOrEqual(cmp) || !toInclusive && greater(cmp))) {
                    setNextEntry();
                    return true;
                }
            }

            // Key is out of range, mark the iterator has reached the end
            setNextEntryNull();
            return false;
        }

        private void setNextEntryNull() {
            lastSlot = NULL_SLOT;
            nextSlot = NULL_SLOT;
            nextEntry = null;
            nextIndexKey = null;
        }

        private void setNextEntry() {
            Data lastIndexKey = leafNodeAccessor.getIndexKeyHeapData(currentNodeAddr, nextSlot);
            NativeMemoryData lastEntryKey = leafNodeAccessor.getEntryKey(currentNodeAddr, nextSlot);
            NativeMemoryData lastValue = leafNodeAccessor.getValue(currentNodeAddr, nextSlot);
            nextEntry = mapEntryFactory.create(lastEntryKey, lastValue);

            nextIndexKey = lastIndexKey;
        }
    }

    /**
     * A batching entry iterator which collects results in a batch.
     * <p>
     * Optimizes the number of times the readLock/releaseLock logic is called
     * and reduces the number of potential iterator re-synchronization if
     * concurrent updates modify the same B+tree node(s).
     */
    class BatchingEntryIterator implements Iterator<T> {

        private final EntryBatch<T> batch;
        private final EntryIterator entryIterator;
        private Iterator<T> batchIterator;

        BatchingEntryIterator(EntryIterator iterator, EntryBatch<T> batch) {
            this.batch = batch;
            this.entryIterator = iterator;
            this.batchIterator = batch.iterator();
        }

        @Override
        public boolean hasNext() {
            if (batchIterator.hasNext()) {
                return true;
            } else {
                batch.clear();
                nextBatch();
                batchIterator = batch.iterator();
                return batchIterator.hasNext();
            }
        }

        private void nextBatch() {
            LockingContext lockingContext = getLockingContext();
            try {
                entryIterator.nextBatch0(batch, lockingContext);
            } catch (Throwable e) {
                releaseLocks(lockingContext);
                throw e;
            } finally {
                assert lockingContext.hasNoLocks();
            }
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return batchIterator.next();
        }
    }

    private static class EntryBatch<T> {

        private final List<T> values;
        private final int capacity;

        EntryBatch(int initialSize, int capacity) {
            this.capacity = capacity;
            this.values = new ArrayList<>(initialSize);
        }

        boolean hasCapacity() {
            return values.size() < capacity;
        }

        void add(T e) {
            values.add(e);
        }

        Iterator<T> iterator() {
            return values.iterator();
        }

        void clear() {
            values.clear();
        }

        int size() {
            return values.size();
        }
    }

    /**
     * Wraps the entries iterator and returns only index keys skipping duplicates.
     */
    private class KeyIterator implements Iterator<Data> {

        private final EntryIterator entryIterator;
        private Data lastKey;

        KeyIterator(EntryIterator entryIterator) {
            this.entryIterator = entryIterator;
        }

        @Override
        public boolean hasNext() {
            if (lastKey == null) {
                return entryIterator.hasNext();
            } else {
                while (entryIterator.hasNext()) {

                    Data indexKeyData = entryIterator.nextIndexKey;
                    if (!indexKeyData.equals(lastKey)) {
                        return true;
                    }
                    // skip the duplicate
                    entryIterator.next();
                }
                return false;
            }
        }

        @Override
        public Data next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            entryIterator.next();
            lastKey = entryIterator.lastIndexKey;
            return lastKey;
        }
    }

    @Override
    public void clear() {
        LockingContext lockingContext = getLockingContext();
        try {
            clear0(lockingContext);
        } catch (Throwable e) {
            releaseLocks(lockingContext);
            throw e;
        } finally {
            assert lockingContext.hasNoLocks();
        }
    }

    /**
     * Removes all entries from the B+tree node creating empty one-level B+tree
     *
     * @param lockingContext the locking context
     */
    private void clear0(LockingContext lockingContext) {
        HDBTreeNodeBaseAccessor nodeBaseAccessor = leafNodeAccessor;

        // The mutex prohibits concurrent clear()
        synchronized (clearMutex) {
            List<Long> nodes = new ArrayList<>();
            // Collects all nodes
            collectNodesForRemove(rootAddr, nodes, lockingContext);

            // Dispose all indexKey/entryKey components "owned" by the B+Tree
            disposeOwnedData(nodes);

            // Create empty one-level B+Tree
            setNodeLevel(rootAddr, 1);
            setKeysCount(rootAddr, 0);

            long newChildAddr = leafNodeAccessor.newNodeLocked(lockingContext);
            innerNodeAccessor.insert(rootAddr, newChildAddr);

            // Release all locks
            nodes.forEach(addr -> releaseLock(addr, lockingContext));
            releaseLock(newChildAddr, lockingContext);

            // Dispose all removed nodes
            nodes.forEach(addr -> {
                if (addr != rootAddr) {
                    nodeBaseAccessor.disposeNode(addr);
                }
            });
        }
    }

    private void disposeOwnedData(List<Long> nodes) {
        for (long nodeAddr : nodes) {
            if (getNodeLevel(nodeAddr) == 0) {
                leafNodeAccessor.disposeOwnedData(nodeAddr);
            } else {
                innerNodeAccessor.disposeOwnedData(nodeAddr);
            }
        }
    }

    @Override
    public void dispose() {
        LockingContext lockingContext = getLockingContext();
        try {
            dispose0(lockingContext);
        } catch (Throwable e) {
            releaseLocks(lockingContext);
            throw e;
        } finally {
            assert lockingContext.hasNoLocks();
        }
    }

    private void dispose0(LockingContext lockingContext) {
        HDBTreeNodeBaseAccessor nodeBaseAccessor = leafNodeAccessor;

        synchronized (clearMutex) {
            if (isDisposed) {
                return;
            }
            List<Long> nodes = new ArrayList<>();
            collectNodesForRemove(rootAddr, nodes, lockingContext);
            // Dispose all indexKey/entryKey components "owned" by the B+Tree
            disposeOwnedData(nodes);

            // Set isDisposed to true before releasing the locks
            isDisposed = true;
            nodes.forEach(addr -> releaseLock(addr, lockingContext));
            nodes.forEach(addr -> nodeBaseAccessor.disposeNode(addr));
        }
    }

    /**
     * Collecting all nodes of the B+tree
     *
     * @param nodeAddr       initially the root address
     * @param nodes          the list of collected nodes
     * @param lockingContext the locking context
     */
    private void collectNodesForRemove(long nodeAddr, List<Long> nodes, LockingContext lockingContext) {
        writeLock(nodeAddr, lockingContext);
        nodes.add(nodeAddr);
        int nodeLevel = getNodeLevel(nodeAddr);
        assert nodeLevel >= 1;
        int keysCount = getKeysCount(nodeAddr);

        for (int i = 0; i <= keysCount; ++i) {
            long childAddr = innerNodeAccessor.getValueAddr(nodeAddr, i);
            if (nodeLevel > 1) {
                collectNodesForRemove(childAddr, nodes, lockingContext);
            } else {
                writeLock(childAddr, lockingContext);
                nodes.add(childAddr);
                incSequenceCounter(childAddr);
            }
        }
        incSequenceCounter(nodeAddr);
    }

    private static LockingContext getLockingContext() {
        LockingContext context = LOCKING_CONTEXT.get();
        assert context.hasNoLocks();
        return context;
    }

    private static LockingContext newLockingContext() {
        return new LockingContext();
    }

    private void readLock(long nodeAddr, LockingContext lockingContext) {
        long lockAddr = getLockStateAddr(nodeAddr);
        lockManager.readLock(lockAddr);
        lockingContext.addLock(lockAddr);
        checkIfDisposed();
    }

    private boolean tryReadLock(long nodeAddr, LockingContext lockingContext) {
        long lockAddr = getLockStateAddr(nodeAddr);
        if (lockManager.tryReadLock(lockAddr)) {
            lockingContext.addLock(lockAddr);
            return true;
        }
        return false;
    }

    private void instantDurationReadLock(long nodeAddr) {
        long lockAddr = getLockStateAddr(nodeAddr);
        lockManager.instantDurationReadLock(lockAddr);
    }

    private void releaseLock(long nodeAddr, LockingContext lockingContext) {
        long lockAddr = getLockStateAddr(nodeAddr);
        lockManager.releaseLock(lockAddr);
        lockingContext.removeLock(lockAddr);
    }

    private void writeLock(long nodeAddr, LockingContext lockingContext) {
        long lockAddr = getLockStateAddr(nodeAddr);
        lockManager.writeLock(lockAddr);
        lockingContext.addLock(lockAddr);
        checkIfDisposed();
    }

    private boolean upgradeToWriteLock(long nodeAddr) {
        long lockAddr = getLockStateAddr(nodeAddr);
        // No need to update locking context
        return lockManager.tryUpgradeToWriteLock(lockAddr);
    }

    private void instantDurationWriteLock(long nodeAddr) {
        long lockAddr = getLockStateAddr(nodeAddr);
        lockManager.instantDurationWriteLock(lockAddr);
    }

    private boolean tryWriteLock(long nodeAddr, LockingContext lockingContext) {
        long lockAddr = getLockStateAddr(nodeAddr);
        if (lockManager.tryWriteLock(lockAddr)) {
            lockingContext.addLock(lockAddr);
            return true;
        }
        return false;
    }

    private void releaseLocks(List<Long> nodeAddrs, LockingContext lockingContext) {
        nodeAddrs.forEach(nodeAddr -> releaseLock(nodeAddr, lockingContext));
    }

    private void releaseLocks(LockingContext lockingContext) {
        lockingContext.releaseLocks(lockManager);
    }

    private boolean upgradeToWriteLock(List<Long> nodeAddrs) {
        for (long nodeAddr : nodeAddrs) {
            if (!upgradeToWriteLock(nodeAddr)) {
                return false;
            }
        }
        return true;
    }

    private void checkIfDisposed() {
        if (isDisposed) {
            throw new DistributedObjectDestroyedException("Disposed B+tree cannot be accessed.");
        }
    }

    BPlusTreeKeyComparator getKeyComparator() {
        return leafNodeAccessor.keyComparator;
    }

    // For unit testing only
    void setNodeSplitStrategy(NodeSplitStrategy nodeSplitStrategy) {
        leafNodeAccessor.setNodeSplitStrategy(nodeSplitStrategy);
        innerNodeAccessor.setNodeSplitStrategy(nodeSplitStrategy);
    }

    // For unit testing only
    boolean isDisposed() {
        return isDisposed;
    }
}
