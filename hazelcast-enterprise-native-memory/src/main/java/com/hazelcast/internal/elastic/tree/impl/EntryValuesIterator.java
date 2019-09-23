package com.hazelcast.internal.elastic.tree.impl;

import com.hazelcast.internal.memory.MemoryBlock;

import java.util.Iterator;
import java.util.NoSuchElementException;

/***
 * Iterator to iterate over key's entry values of the off-heap storage structure
 */
public class EntryValuesIterator
        implements Iterator<MemoryBlock> {

    private RedBlackTreeNode.EntryValueNode next;

    EntryValuesIterator(RedBlackTreeNode.Entry entry) {
        this.next = entry.getValuesHead();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public MemoryBlock next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No next element for iteration");
        }

        try {
            return next.value();
        } finally {
            next = next.next();
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
