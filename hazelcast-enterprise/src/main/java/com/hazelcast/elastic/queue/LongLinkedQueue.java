package com.hazelcast.elastic.queue;

import com.hazelcast.elastic.LongIterator;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.nio.UnsafeHelper;
import sun.misc.Unsafe;

import java.util.NoSuchElementException;

public final class LongLinkedQueue implements LongQueue {

    private static final long NULL_PTR = 0L;

    private static final int NODE_SIZE = 16;

    private static final int NEXT_OFFSET = 8;

    private final MemoryAllocator malloc;

    private final long nullItem;

    private final int capacity;

    private final boolean hasCapacity;

    private long head;

    private long tail;

    private int size;

    public LongLinkedQueue(final MemoryAllocator malloc, final int capacity, final long nullValue) {
        this.malloc = malloc;
        this.capacity = capacity;
        this.hasCapacity =  capacity < Integer.MAX_VALUE;
        nullItem = nullValue;
        tail = head = newNode(nullItem);
    }

    public LongLinkedQueue(MemoryAllocator memoryAllocator, long nullValue) {
        this(memoryAllocator, Integer.MAX_VALUE, nullValue);
    }

    private long newNode(final long e) {
        Unsafe unsafe = UnsafeHelper.UNSAFE;
        long address = malloc.allocate(NODE_SIZE);
        unsafe.putLong(null, address, e);
        unsafe.putLong(null, address + NEXT_OFFSET, NULL_PTR);
        return address;
    }

    private long getItem(long node) {
        assert node != NULL_PTR;
        return UnsafeHelper.UNSAFE.getLong(node);
    }

    private long getNextNode(long node) {
        assert node != NULL_PTR;
        return UnsafeHelper.UNSAFE.getLong(node + NEXT_OFFSET);
    }

    private void setNextNode(long node, long value) {
        assert node != NULL_PTR;
        UnsafeHelper.UNSAFE.putLong(node + NEXT_OFFSET, value);
    }

    public boolean offer(long value) {
        assert value != nullItem;

        if (hasCapacity && size == capacity) {
            return false;
        }
        long node = newNode(value);
        setNextNode(tail, node);
        tail = node;
        size++;
        return true;
    }

    public long poll() {
        if (size == 0) {
            return nullItem;
        }
        long item;
        long node;
        node = head;
        long newHead = getNextNode(node);
        if (newHead == NULL_PTR) {
            return nullItem;
        }
        item = getItem(newHead);
        head = newHead;
        size--;
        malloc.free(node, NODE_SIZE);
        return item;
    }

    public long peek() {
        if (size == 0) {
            return nullItem;
        }
        long first = getNextNode(head);
        if (first == NULL_PTR) {
            return nullItem;
        } else {
            return getItem(first);
        }
    }

    public int size() {
        long c = size;
        return c < Integer.MAX_VALUE ? (int) c : Integer.MAX_VALUE;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public int capacity() {
        return capacity;
    }

    public int remainingCapacity() {
        return hasCapacity ? capacity - size() : Integer.MAX_VALUE ;
    }

    public void clear() {
        while (poll() != nullItem);
    }

    public void destroy() {
        if (head != NULL_PTR) {
            clear();
            long ptr = head;
            long ptr2 = tail;
            assert ptr == ptr2;
            if (ptr != NULL_PTR) {
                malloc.free(ptr, NODE_SIZE);
            }
        }
    }

    public long nullItem() {
        return nullItem;
    }

    public LongIterator iterator() {
        return new Iter();
    }

    private class Iter implements LongIterator {
        long currentNode;

        private Iter() {
            reset();
        }

        public boolean hasNext() {
            return currentNode != NULL_PTR;
        }

        public long next() {
            if (currentNode == NULL_PTR) {
                throw new NoSuchElementException();
            }
            long item = getItem(currentNode);
            currentNode = getNextNode(currentNode);
            return item;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset() {
            currentNode = getNextNode(head);
        }
    }
}


