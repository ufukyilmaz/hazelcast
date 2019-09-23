package com.hazelcast.internal.elastic.tree.impl;

import com.hazelcast.internal.elastic.tree.OffHeapTreeEntry;
import com.hazelcast.internal.elastic.tree.OrderingDirection;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.hazelcast.internal.elastic.tree.OrderingDirection.ASC;
import static com.hazelcast.internal.elastic.tree.OrderingDirection.DESC;
import static com.hazelcast.internal.elastic.tree.impl.RedBlackTreeNode.LEFT;
import static com.hazelcast.internal.elastic.tree.impl.RedBlackTreeNode.RIGHT;

/***
 * Iterator to iterate over keys of the off-heap storage structure;
 */
public class EntryIterator
        implements Iterator<OffHeapTreeEntry> {

    private RedBlackTreeNode currNode;
    private OrderingDirection direction;

    EntryIterator(RedBlackTreeNode root, OrderingDirection direction) {
        this.direction = direction;
        this.currNode = root != null ? first(root) : null;
    }

    @Override
    public boolean hasNext() {
        return currNode != null && !currNode.isNil();
    }

    @Override
    public OffHeapTreeEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        OffHeapTreeEntry result = currNode.entry();
        currNode = next(currNode);
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    private boolean checkSide(byte side, OrderingDirection direction) {
        return (((side == LEFT) && (direction == ASC)) || ((side == RIGHT) && (direction == DESC)));
    }

    private RedBlackTreeNode next(RedBlackTreeNode node) {
        RedBlackTreeNode child = direction == ASC
                ? node.right()
                : node.left();

        if (!child.isNil()) {
            return first(child);
        }

        byte side = node.side();

        if (checkSide(side, direction)) {
            RedBlackTreeNode parent = node.parent();

            if (parent != null) {
                return parent;
            }
        }

        while (!checkSide(side, direction)) {
            RedBlackTreeNode parent = node.parent();

            if (parent == null) {
                return null;
            } else {
                node = parent;
                side = node.side();
            }
        }

        return node.parent();
    }

    private RedBlackTreeNode first(RedBlackTreeNode node) {
        if (node.isNil()) {
            return null;
        }

        RedBlackTreeNode current = node;

        while (true) {
            RedBlackTreeNode next;

            if (direction == ASC) {
                next = current.left();
            } else {
                next = current.right();
            }

            if (next.isNil()) {
                return current;
            }

            current = next;
        }
    }
}
