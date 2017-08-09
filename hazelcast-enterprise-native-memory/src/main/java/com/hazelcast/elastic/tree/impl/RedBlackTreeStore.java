package com.hazelcast.elastic.tree.impl;

import com.hazelcast.elastic.tree.OffHeapComparator;
import com.hazelcast.elastic.tree.OffHeapTreeEntry;
import com.hazelcast.elastic.tree.OffHeapTreeStore;
import com.hazelcast.elastic.tree.OrderingDirection;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.memory.MemoryBlock;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.hazelcast.elastic.tree.OrderingDirection.ASC;
import static com.hazelcast.elastic.tree.impl.RedBlackTreeNode.BLACK;
import static com.hazelcast.elastic.tree.impl.RedBlackTreeNode.LEFT;
import static com.hazelcast.elastic.tree.impl.RedBlackTreeNode.RED;
import static com.hazelcast.elastic.tree.impl.RedBlackTreeNode.RIGHT;
import static com.hazelcast.elastic.tree.impl.RedBlackTreeNode.newNode;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.util.ExceptionUtil.rethrow;

/***
 * This is a classic Red Black tree implementation using off-heap memory.
 * Every node can hold multiple values in linked-list approach.
 * The algorithm is based on the CLRS book, Introduction to Algorithms.
 */
@SuppressWarnings({"checkstyle:magicnumber", "checkstyle:methodcount", "checkstyle:npathcomplexity",
                   "checkstyle:cyclomaticcomplexity", "checkstyle:methodlength", "checkstyle:nestedifdepth",
                   "checkstyle:innerassignment", "checkstyle:returncount"})
public class RedBlackTreeStore
        implements OffHeapTreeStore {

    private static final RedBlackTreeNode NIL = new RedBlackTreeNode();

    private boolean assertOn;

    private final MemoryAllocator malloc;
    private final OffHeapComparator offHeapKeyComparator;

    private RedBlackTreeNode root;

    public RedBlackTreeStore(
            MemoryAllocator malloc,
            OffHeapComparator offHeapKeyComparator) {
        this(malloc, offHeapKeyComparator, false);
    }

    @SuppressWarnings("checkstyle:simplifybooleanexpression")
    @SuppressFBWarnings({"UCF_USELESS_CONTROL_FLOW_NEXT_LINE"})
    public RedBlackTreeStore(
            MemoryAllocator malloc,
            OffHeapComparator offHeapKeyComparator, boolean disableConcistencyAssertions) {
        this.malloc = malloc;
        this.offHeapKeyComparator = offHeapKeyComparator;
        assert (assertOn = !disableConcistencyAssertions) || true;
    }

    public OffHeapTreeEntry put(MemoryBlock key, MemoryBlock value) {
        return put(key, value, offHeapKeyComparator);
    }

    public OffHeapTreeEntry put(MemoryBlock key, MemoryBlock value, OffHeapComparator comparator) {
        checkNotNull(key);
        checkNotNull(value);

        try {
            if (root == null) {
                setRoot(createNewNode(key, value));
                return root.entry();
            }

            LookupResult result = lookup(key, comparator);
            if (result.isExactMatch) {
                RedBlackTreeNode.Entry entry = (RedBlackTreeNode.Entry) result.node.entry();
                entry.addValue(value);
                return entry;
            }

            RedBlackTreeNode node = createNewLeaf(key, value, result.node, result.side);
            return node.entry();
        } finally {
            assertNoInconsistencies();
        }
    }

    public OffHeapTreeEntry getEntry(MemoryBlock key) {
        return this.getEntry(key, offHeapKeyComparator);
    }

    public OffHeapTreeEntry getEntry(MemoryBlock key, OffHeapComparator comparator) {
        LookupResult result = lookup(key, comparator == null ? offHeapKeyComparator : comparator);
        return result.isExactMatch && result.node != null ? result.node.entry() : null;
    }

    public OffHeapTreeEntry searchEntry(MemoryBlock key) {
        LookupResult result = lookup(key, offHeapKeyComparator);
        return result.node != null ? result.node.entry() : null;
    }

    public void dispose(boolean releasePayload) {
        if (root != null) {
            root.dispose(releasePayload);
            root = null;
        }
    }

    public void remove(OffHeapTreeEntry entry) {
        RedBlackTreeNode node = ((RedBlackTreeNode.Entry) entry).node();
        if (node.isNil()) {
            throw new IllegalArgumentException("Invalid entry.");
        }

        byte origColor = node.color();

        RedBlackTreeNode child;
        if (node.left().isNil()) {
            child = node.right();
            transplant(node, child);
        } else if (node.right().isNil()) {
            child = node.left();
            transplant(node, child);
        } else {
            RedBlackTreeNode minChild = treeMin(node.right());
            origColor = minChild.color();
            child = minChild.right();

            if (!minChild.parent().equals(node)) {
                transplant(minChild, minChild.right());
                minChild.right(node.right());

                RedBlackTreeNode rightMinGrandChild = minChild.right();
                if (!rightMinGrandChild.isNil()) {
                    rightMinGrandChild.parent(minChild);
                }
            }

            transplant(node, minChild);

            RedBlackTreeNode leftChild = node.left();
            leftChild.parent(minChild);
            minChild.left(leftChild);
            minChild.color(node.color());
        }

        if (origColor == BLACK && !child.isNil()) {
            removeFixUp(child);
        }

        // Clear refs
        node.clearSides();

        // Release memory
        node.dispose();
        assertNoInconsistencies();
    }

    @Override
    public Iterator<OffHeapTreeEntry> iterator() {
        return entries();
    }

    @Override
    public Iterator<OffHeapTreeEntry> entries() {
        return entries(ASC);
    }

    @Override
    public Iterator<OffHeapTreeEntry> entries(OrderingDirection direction) {
        return new EntryIterator(root, direction);
    }

    @Override
    public Iterator<OffHeapTreeEntry> entries(OffHeapTreeEntry root) {
        return entries(root, ASC);
    }

    @Override
    public Iterator<OffHeapTreeEntry> entries(OffHeapTreeEntry entry, OrderingDirection direction) {
        return new EntryIterator(((RedBlackTreeNode.Entry) entry).node(), direction);
    }

    private void checkNotNull(MemoryBlock blob) {
        if (blob == null || blob.address() == NULL_ADDRESS) {
            throw new IllegalArgumentException("Null blobs or null-address based, not allowed.");
        }
    }
    private void setRoot(RedBlackTreeNode node) {
        root = node.isNil() ? null : node;
        if (root != null) {
            root.parent(null);
        }
    }

    private LookupResult lookup(MemoryBlock key, OffHeapComparator comparator) {
        checkNotNull(key);

        if (root == null) {
            return new LookupResult(null, true);
        }

        RedBlackTreeNode node = root;

        while (true) {
            int compareResult = compareKeys(comparator, key, node);
            if (compareResult > 0) {
                //Our key is greater
                RedBlackTreeNode right = node.right();

                if (right.isNil()) {
                    return new LookupResult(node, false, RIGHT);
                } else {
                    node = right;
                }
            } else if (compareResult < 0) {
                //Our key is less
                RedBlackTreeNode left = node.left();

                if (left.isNil()) {
                    return new LookupResult(node, false, LEFT);
                } else {
                    node = left;
                }
            } else {
                //Our key is the same
                return new LookupResult(node, true);
            }
        }
    }

    private RedBlackTreeNode createNewNode(MemoryBlock key, MemoryBlock value) {
        RedBlackTreeNode node = null;
        try {
            node = newNode(this, malloc);
            RedBlackTreeNode.Entry entry = ((RedBlackTreeNode.Entry) node.entry());
            entry.setKey(key);
            entry.addValue(value);

            return node;
        } catch (Exception ex) {
            if (node != null) {
                node.dispose();
            }

            throw rethrow(ex);
        }
    }

    private RedBlackTreeNode createNewLeaf(MemoryBlock key, MemoryBlock value, RedBlackTreeNode parent, byte side) {
        RedBlackTreeNode node = createNewNode(key, value);

        node.color(RED);
        node.parent(parent);

        if (side == LEFT) {
            parent.left(node);
        } else {
            parent.right(node);
        }

        checkRedBlackConsistency(parent, node, side);
        return node;
    }

    private void checkRedBlackConsistency(RedBlackTreeNode father, RedBlackTreeNode son, byte sonSide) {
        if (father == null) {
            root.color(BLACK);
            return;
        }

        RedBlackTreeNode grandFather = father.parent();

        if (father.color() == BLACK) {
            return;
        }

        byte fathersSide = father.side();
        RedBlackTreeNode uncle;

        if (fathersSide == LEFT) {
            uncle = grandFather.right();
        } else {
            uncle = grandFather.left();
        }

        if (!uncle.isNil()) {
            //Case 1 - red uncle
            if (case1(father, grandFather, uncle)) {
                return;
            }
        }

        //Case 2: Son's and father's side are different, uncle is black or absent
        if ((sonSide != fathersSide)) {
            case2(father, son, sonSide, grandFather, fathersSide);

            //Switch father and son address
            RedBlackTreeNode tmp = father;
            father = son;
            son = tmp;

            sonSide = son.side();
            fathersSide = father.side();
        }

        //Case 3: Son's and father's side are the same, uncle is black or absent
        if (sonSide == fathersSide) {
            if (case3(father, sonSide, grandFather, fathersSide)) {
                return;
            }
        }

        root.color(BLACK);
    }

    private boolean case3(RedBlackTreeNode father, byte sonSide,
                          RedBlackTreeNode grandFather, byte fathersSide) {

        if (grandFather.isNil()) {
            return true;
        }

        RedBlackTreeNode grandFathersParent = grandFather.parent();

        //Grandfather's migration
        if (grandFathersParent == null) {
            setRoot(father);
        } else {
            byte grandFathersSide = grandFather.side();

            if (grandFathersSide == LEFT) {
                grandFathersParent.left(father);
            } else {
                grandFathersParent.right(father);
            }

            father.parent(grandFathersParent);
        }

        RedBlackTreeNode fathersRight = father.right();
        RedBlackTreeNode fathersLeft = father.left();

        // Father link to grandfather
        // Grandfather links to father's another child
        if (sonSide == LEFT) {
            father.right(grandFather);
            grandFather.left(fathersRight);
            grandFather.parent(father);

            if (!fathersRight.isNil()) {
                fathersRight.parent(grandFather);
            }
        } else {
            father.left(grandFather);
            grandFather.right(fathersLeft);
            grandFather.parent(father);

            if (!fathersLeft.isNil()) {
                fathersLeft.parent(grandFather);
            }
        }

        // Changing color
        // GrandFather - red
        grandFather.color(RED);
        father.color(BLACK);
        grandFather.side(fathersSide == LEFT ? RIGHT : LEFT);
        return false;
    }

    private void case2(RedBlackTreeNode father, RedBlackTreeNode son, byte sonSide,
                       RedBlackTreeNode grandFather, byte fathersSide) {

        if (fathersSide == LEFT) {
            grandFather.left(son);
        } else {
            grandFather.right(son);
        }

        RedBlackTreeNode sonsChild = sonSide == RIGHT
                ? son.left()
                : son.right();

        //Son's left becomes father's right
        if (sonSide == RIGHT) {
            father.right(sonsChild);
        } else {
            father.left(sonsChild);
        }

        if (!sonsChild.isNil()) {
            sonsChild.parent(father);
        }

        if (sonSide == RIGHT) {
            son.left(father);
        } else {
            son.right(father);
        }


        father.parent(son);
        son.parent(grandFather);
    }

    private boolean case1(RedBlackTreeNode father, RedBlackTreeNode grandfather, RedBlackTreeNode uncle) {
        byte uncleColor = uncle.color();

        if (uncleColor == RED) {
            uncle.color(BLACK);
            father.color(BLACK);

            if (!grandfather.isNil()) {
                grandfather.color(RED);

                byte grandFatherSide = grandfather.side();
                RedBlackTreeNode grandFatherParent = grandfather.parent();

                // Recursive call
                checkRedBlackConsistency(grandFatherParent, grandfather, grandFatherSide);
            } else {
                root.color(BLACK);
            }

            return true;
        }

        return false;
    }

    private int compareKeys(OffHeapComparator comparator, MemoryBlock key, RedBlackTreeNode node) {
        MemoryBlock against = node.entry().getKey();
        return comparator.compare(key, against);
    }

    private RedBlackTreeNode treeMin(RedBlackTreeNode entry) {
        RedBlackTreeNode minEntry = entry;

        RedBlackTreeNode leftChild;
        while (!(leftChild = minEntry.left()).isNil()) {
            minEntry = leftChild;
        }

        return minEntry;
    }

    private void transplant(RedBlackTreeNode which, RedBlackTreeNode with) {
        assert !which.equals(with);

        if (root.equals(which)) {
            setRoot(with);
            return;
        }

        RedBlackTreeNode parent = which.parent();

        assert parent != null : "Parent can't be NIL since src isn't ROOT. "
                + "root: " + root + ", src: " + which + ", root_parent: " + root.parent();

        RedBlackTreeNode entryParentLeftChildAddress = parent.left();
        if (which.equals(entryParentLeftChildAddress)) {
            assert !which.equals(parent.right());
            parent.left(with);
        } else {
            parent.right(with);
        }

        if (!with.isNil()) {
            with.parent(parent);
        }
    }

    private void rotateRight(RedBlackTreeNode node) {
        RedBlackTreeNode parent = node.parent();
        RedBlackTreeNode leftChild = node.left();

        if (leftChild.isNil()) {
            return;
        }

        RedBlackTreeNode leftChildsRightChild = leftChild.right();
        node.left(leftChildsRightChild);

        if (!leftChildsRightChild.isNil()) {
            leftChildsRightChild.parent(node);
        }

        leftChild.parent(parent);

        if (root.equals(node)) {
            setRoot(leftChild);
        } else if (parent.right().equals(node)) {
            parent.right(leftChild);
        } else {
            parent.left(leftChild);
        }

        leftChild.right(node);
        node.parent(leftChild);
    }

    private void rotateLeft(RedBlackTreeNode node) {
        RedBlackTreeNode parent = node.parent();
        RedBlackTreeNode rightChild = node.right();

        if (rightChild.isNil()) {
            return;
        }

        RedBlackTreeNode rightChildsLeftChild = rightChild.left();
        node.right(rightChildsLeftChild);

        if (!rightChildsLeftChild.isNil()) {
            rightChildsLeftChild.parent(node);
        }

        rightChild.parent(parent);

        if (root.equals(node)) {
            setRoot(rightChild);
        } else if (parent.left().equals(node)) {
            parent.left(rightChild);
        } else {
            parent.right(rightChild);
        }

        rightChild.left(node);
        node.parent(rightChild);
    }

    private void removeFixUp(RedBlackTreeNode node) {
        while (!node.isNil() && !node.equals(root) && node.color() == BLACK) {

            RedBlackTreeNode father = node.parent();

            RedBlackTreeNode fathersLeftChild = father.left();
            RedBlackTreeNode fathersRightChild = father.right();

            if (node.equals(fathersLeftChild)) {
                // Case 1, Siblings color is RED
                if (fathersRightChild.color() == RED) {
                    fathersRightChild.color(BLACK);
                    father.color(RED);
                    rotateLeft(father);
                    fathersRightChild = father.right();
                }

                RedBlackTreeNode fathersRightChildsLeftChild = fathersRightChild.isNil() ? NIL : fathersRightChild.left();
                RedBlackTreeNode fathersRightChildsRightChild = fathersRightChild.isNil() ? NIL : fathersRightChild.right();

                // Case 2, Sibling's color is BLACK and so are his children
                if (!fathersRightChild.isNil()
                        && (fathersRightChildsLeftChild.color() == BLACK)
                        && (fathersRightChildsRightChild.color() == BLACK)) {

                    fathersRightChild.color(RED);
                    node = father;
                } else {
                    if (!fathersRightChild.isNil() && (fathersRightChildsRightChild.color() == BLACK)) {
                        if (!fathersRightChildsLeftChild.isNil()) {
                            fathersRightChildsLeftChild.color(BLACK);
                        }

                        fathersRightChild.color(RED);
                        rotateRight(fathersRightChild);
                        fathersRightChild = father.right();
                        fathersRightChildsRightChild = fathersRightChild.isNil() ? NIL : fathersRightChild.right();
                        fathersRightChildsLeftChild = fathersRightChild.isNil() ? NIL : fathersRightChild.left();
                    }

                    if (!fathersRightChildsLeftChild.isNil()) {
                        fathersRightChild.color(father.color());
                        if (!fathersRightChildsRightChild.isNil()) {
                            fathersRightChildsRightChild.color(BLACK);
                        }
                    }

                    father.color(BLACK);
                    rotateLeft(father);
                    node = root;
                }
            } else {
                // Case 1, Siblings color is RED
                if (fathersLeftChild.color() == RED) {
                    fathersLeftChild.color(BLACK);
                    father.color(RED);

                    rotateRight(father);
                    fathersLeftChild = father.left();
                }

                RedBlackTreeNode fathersLeftChildsLeftChild = fathersLeftChild.isNil() ? NIL : fathersLeftChild.left();
                RedBlackTreeNode fathersLeftChildsRightChild = fathersLeftChild.isNil() ? NIL : fathersLeftChild.right();

                // Case 2, Sibling's color is BLACK and so are his children
                if (!fathersLeftChild.isNil()
                        && ((fathersLeftChildsLeftChild.color() == BLACK)
                        &&  (fathersLeftChildsRightChild.color() == BLACK))) {
                    fathersLeftChild.color(RED);
                    node = father;
                } else {
                    // Case 3-4,
                    if (!fathersLeftChild.isNil() && (fathersLeftChildsRightChild.color() == BLACK)) {

                        if (!fathersLeftChildsLeftChild.isNil()) {
                            fathersLeftChildsLeftChild.color(BLACK);
                        }

                        fathersLeftChild.color(RED);
                        rotateLeft(fathersLeftChild);

                        fathersLeftChild = father.left();
                        fathersLeftChildsRightChild = fathersLeftChild.isNil() ? NIL : fathersLeftChild.right();
                        fathersLeftChildsLeftChild = fathersLeftChild.isNil() ? NIL : fathersLeftChild.left();
                    }

                    if (!fathersLeftChildsLeftChild.isNil()) {
                        fathersLeftChild.color(father.color());

                        if (!fathersLeftChildsRightChild.isNil()) {
                            fathersLeftChildsRightChild.color(BLACK);
                        }
                    }

                    father.color(BLACK);
                    rotateRight(father);
                    node = root;
                }
            }
        }

        node.color(BLACK);
    }

    private void assertNoInconsistencies() {
        if (!assertOn) {
            return;
        }

        Set<OffHeapTreeEntry> entries = new HashSet<OffHeapTreeEntry>();
        Set<MemoryBlock> keys = new HashSet<MemoryBlock>();

        for (OffHeapTreeEntry entry : this) {
            assert !entries.contains(entry) : "Tree in illegal state, entry: " + entry + " exists more than once.";

            entries.add(entry);

            assert !keys.contains(entry.getKey()) : "Tree in illegal state, entry key: " + entry.getKey()
                    + " is referenced more than once.";

            keys.add(entry.getKey());

            assert entry.hasValues() : "Tree in illegal state, entry: " + entry + " has no values.";
        }
    }

    private static final class LookupResult {

        private final RedBlackTreeNode node;
        private final boolean isExactMatch;
        private final byte side;

        private LookupResult(RedBlackTreeNode node, boolean isExactMatch) {
            this(node, isExactMatch, (byte) 0xff);
        }

        private LookupResult(RedBlackTreeNode node, boolean isExactMatch, byte side) {
            this.node = node;
            this.isExactMatch = isExactMatch;
            this.side = side;
        }
    }
}
