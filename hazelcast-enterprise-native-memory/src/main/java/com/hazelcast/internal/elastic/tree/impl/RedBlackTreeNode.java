package com.hazelcast.internal.elastic.tree.impl;

import com.hazelcast.internal.elastic.tree.OffHeapTreeEntry;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryBlock;

import java.util.Iterator;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

@SuppressWarnings({"checkstyle:innerassignment"})
class RedBlackTreeNode
    extends MemoryBlock {

    // Tree
    static final byte RED = 1;

    static final byte BLACK = 0;

    static final byte LEFT = 1;

    static final byte RIGHT = 0;

    // Node
    private static final int LEFT_LEAF_OFFSET = 0;

    private static final int RIGHT_LEAF_OFFSET = 8;

    private static final int ENTRY_KEY_OFFSET = 16;

    private static final int ENTRY_KEY_SZ_OFFSET = 24;

    private static final int ENTRY_VALUE_OFFSET = 28;

    private static final int PARENT_OFFSET = 36;

    private static final int SIDE_OFFSET = 44;

    private static final int COLOR_OFFSET = 45;

    private static final int NODE_SIZE = COLOR_OFFSET + 1;

    private final RedBlackTreeStore tree;

    private final MemoryAllocator malloc;

    private RedBlackTreeNode(RedBlackTreeStore tree, MemoryAllocator malloc, long address) {
        super(address, NODE_SIZE);
        this.tree = tree;
        this.malloc = malloc;
    }

    RedBlackTreeNode() {
        super(NULL_ADDRESS, NODE_SIZE);
        this.tree = null;
        this.malloc = null;
    }

    public OffHeapTreeEntry entry() {
        if (address == NULL_ADDRESS) {
            throw new IllegalStateException("NULL address; ie. Sentinel node");
        }

        return new Entry();
    }

    RedBlackTreeNode parent() {
        long addr;
        if (address == NULL_ADDRESS || (addr = readLong(PARENT_OFFSET)) == NULL_ADDRESS) {
            return null;
        }

        return of(tree, malloc, addr);
    }

    void parent(RedBlackTreeNode node) {
        assert address != NULL_ADDRESS;

        if (node == null) {
            writeLong(PARENT_OFFSET, NULL_ADDRESS);
            return;
        }


        assert node.address != address : "Detected circular reference: " + address;

        writeLong(PARENT_OFFSET, node.address);
    }

    /**
     * The left node of this node, which could also be NIL
     * NIL is allowed on purpose, see sentinel nodes on CLR implementation
     * @return The left node {@link RedBlackTreeNode}
     */
    RedBlackTreeNode left() {
        return of(tree, malloc, leftAddress());
    }

    long leftAddress() {
        assert address != NULL_ADDRESS;
        return readLong(LEFT_LEAF_OFFSET);
    }

    void left(RedBlackTreeNode node) {
        assert address != NULL_ADDRESS;
        assert node.address != address : "Detected circular reference: " + address;

        writeLong(LEFT_LEAF_OFFSET, node.address);
        if (!node.isNil()) {
            node.side(LEFT);
        }
    }

    /**
     * The right node of this node, which could also be NIL
     * NIL is allowed on purpose, see sentinel nodes on CLR implementation
     * @return The right node {@link RedBlackTreeNode}
     */
    RedBlackTreeNode right() {
        return of(tree, malloc, rightAddress());
    }

    long rightAddress() {
        assert address != NULL_ADDRESS;
        return readLong(RIGHT_LEAF_OFFSET);
    }

    void right(RedBlackTreeNode node) {
        assert address != NULL_ADDRESS;
        assert node.address != address : "Detected circular reference: " + address;

        writeLong(RIGHT_LEAF_OFFSET, node.address);

        if (!node.isNil()) {
            node.side(RIGHT);
        }
    }

    void clearSides() {
        assert address != NULL_ADDRESS;

        writeLong(LEFT_LEAF_OFFSET, NULL_ADDRESS);
        writeLong(RIGHT_LEAF_OFFSET, NULL_ADDRESS);
    }

    /**
     * The color of the current node, black or red.
     * NIL nodes, will always be black, see sentinel nodes on CLR implementation.
     * @return The color of the node
     */
    byte color() {
        return address == NULL_ADDRESS
                ? BLACK
                : readByte(COLOR_OFFSET);
    }

    void color(byte color) {
        if (color != RED && color != BLACK) {
            throw new IllegalArgumentException("Unsupported tree-node color: " + color);
        }

        assert address != NULL_ADDRESS;
        writeByte(COLOR_OFFSET, color);
    }

    byte side() {
        assert address != NULL_ADDRESS;
        return readByte(SIDE_OFFSET);
    }

    void side(byte side) {
        if (side != LEFT && side != RIGHT) {
            throw new IllegalArgumentException("Unsupported tree-node side: " + side);
        }

        assert address != NULL_ADDRESS;
        writeByte(SIDE_OFFSET, side);
    }

    boolean isNil() {
        return address == NULL_ADDRESS;
    }

    void reset() {
        reset(NULL_ADDRESS);
    }

    void reset(long baseAddr) {
        setAddress(baseAddr);
    }

    RedBlackTreeNode asNew() {
        return of(tree, malloc, address);
    }

    void dispose() {
        dispose(false);
    }

    void dispose(boolean releasePayload) {
        if (isNil()) {
            return;
        }

        RedBlackTreeNode left = left();
        RedBlackTreeNode right = right();

        if (!left.isNil()) {
            left.dispose(releasePayload);
        }

        if (!right.isNil()) {
            right.dispose(releasePayload);
        }

        disposeEntry(releasePayload);
    }

    private void disposeEntry(boolean releasePayLoad) {
        disposeEntry(releasePayLoad, true);
    }

    private void disposeEntry(boolean releasePayLoad, boolean releaseValue) {
        MemoryBlock key = entry().getKey();

        if ((key.address() != NULL_ADDRESS) && (releasePayLoad)) {
            malloc.free(key.address(), key.size());
        }

        if (releaseValue) {
            disposeValue(releasePayLoad);
        }

        malloc.free(address, NODE_SIZE);
    }

    private void disposeValue(boolean releasePayLoad) {
        Entry entry = (Entry) entry();
        EntryValueNode value = entry.getValuesHead();

        while (value != null) {
            if (releasePayLoad) {
                disposeValuePayload(value);
            }

            EntryValueNode preValue = value;
            value = value.next();

            malloc.free(preValue.address(), preValue.size());
        }
    }

    private void disposeValuePayload(EntryValueNode node) {
        MemoryBlock value = node.value();
        if (value.address() != NULL_ADDRESS) {
            malloc.free(value.address(), value.size());
        }
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return "RedBlackTreeNode{"
                + "address: " + address
                + ", parent: " + readLong(PARENT_OFFSET)
                + ", left: " + readLong(LEFT_LEAF_OFFSET)
                + ", right: " + readLong(RIGHT_LEAF_OFFSET)
                + ", color: " + readByte(COLOR_OFFSET)
                + ", side: " + readByte(SIDE_OFFSET)
                + "}";
    }

    public static RedBlackTreeNode of(RedBlackTreeStore tree, MemoryAllocator malloc, long base) {
        return new RedBlackTreeNode(tree, malloc, base);
    }

    public static RedBlackTreeNode newNode(RedBlackTreeStore tree, MemoryAllocator malloc) {
        long addr = malloc.allocate(NODE_SIZE);
        return of(tree, malloc, addr);
    }

    /**
     * RedBlackTree Node Entry, holding Key & Values for than tree node.
     * Lazy initialized, upon request.
     */
    class Entry
            implements OffHeapTreeEntry {

        RedBlackTreeNode node() {
            return RedBlackTreeNode.this;
        }

        @Override
        public MemoryBlock getKey() {
            assert address != NULL_ADDRESS;

            long address = readLong(ENTRY_KEY_OFFSET);
            int size = readInt(ENTRY_KEY_SZ_OFFSET);

            return address == NULL_ADDRESS
                    ? null
                    : new MemoryBlock(address, size);
        }

        @Override
        public boolean hasValues() {
            return getValuesHead() != null;
        }

        @Override
        public Iterator<MemoryBlock> values() {
            return new EntryValuesIterator(this);
        }

        void setKey(MemoryBlock key) {
            assert address != NULL_ADDRESS;

            writeLong(ENTRY_KEY_OFFSET, key.address());
            writeInt(ENTRY_KEY_SZ_OFFSET, key.size());
        }

        EntryValueNode getValuesHead() {
            assert address != NULL_ADDRESS;

            long address = readLong(ENTRY_VALUE_OFFSET);
            return address == NULL_ADDRESS ? null : new EntryValueNode(address);
        }

        void setValuesHead(EntryValueNode valueNode) {
            writeLong(ENTRY_VALUE_OFFSET, valueNode.address());
        }

        void addValue(MemoryBlock payload) {
            EntryValueNode head = getValuesHead();
            EntryValueNode newVal = null;

            try {
                newVal = newValueNode(payload);

                // If it is first element - setting link on him
                if (head == null || head.isEmpty()) {
                    setValuesHead(newVal);
                    newVal.last(newVal);
                } else {
                    EntryValueNode last = head.last();

                    if (last != null) {
                        last.next(newVal);
                    }

                    head.last(newVal);
                }
            } catch (Exception ex) {
                if (newVal != null) {
                    malloc.free(newVal.address(), newVal.size());
                }

                rethrow(ex);
            }
        }

        EntryValueNode newValueNode(MemoryBlock payload) {
            long address = malloc.allocate(EntryValueNode.VALUE_NODE_SZ);
            EntryValueNode node = new EntryValueNode(address);
            node.zero();
            node.value(payload);

            return node;
        }

        @Override
        public String toString() {
            return "Entry{"
                    + "node: " + node()
                    + ", key: " + readLong(ENTRY_KEY_OFFSET)
                    + ", values: " + readLong(ENTRY_VALUE_OFFSET)
                    + "}";
        }
    }

    /**
     * LinkedList of Entry values
     */
    @SuppressWarnings({"checkstyle:magicnumber"})
    class EntryValueNode
        extends MemoryBlock {

        private static final int VALUE_SZ_OFFSET = 0;

        private static final int VALUE_ADDR_OFFSET = 4;

        private static final int VALUE_NEXT_NODE_OFFSET = 12;

        private static final int VALUE_LAST_NODE_OFFSET = 20;

        private static final int VALUE_NODE_SZ = VALUE_LAST_NODE_OFFSET + 8;

        EntryValueNode(long addr) {
            super(addr, VALUE_NODE_SZ);
        }

        EntryValueNode next() {
            long addr = readLong(VALUE_NEXT_NODE_OFFSET);
            return addr == NULL_ADDRESS ? null : new EntryValueNode(addr);
        }

        void next(EntryValueNode ref) {
            writeLong(VALUE_NEXT_NODE_OFFSET, ref.address);
        }

        EntryValueNode last() {
            long addr = readLong(VALUE_LAST_NODE_OFFSET);
            return addr == NULL_ADDRESS ? null : new EntryValueNode(addr);
        }

        void last(EntryValueNode ref) {
            writeLong(VALUE_LAST_NODE_OFFSET, ref.address);
        }

        MemoryBlock value() {
            return new MemoryBlock(readLong(VALUE_ADDR_OFFSET), readInt(VALUE_SZ_OFFSET));
        }

        void value(MemoryBlock value) {
            writeLong(VALUE_ADDR_OFFSET, value.address());
            writeInt(VALUE_SZ_OFFSET, value.size());
        }

        boolean isEmpty() {
            return address == NULL_ADDRESS;
        }
    }
}
