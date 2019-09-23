package com.hazelcast.internal.elastic.queue;

import com.hazelcast.internal.elastic.LongIterator;
import com.hazelcast.internal.memory.MemoryAllocator;

import java.util.NoSuchElementException;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;

/** Implementation of {@link LongQueue} with a linked list. */
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
        head = newNode(nullItem);
        tail = head;
    }

    public LongLinkedQueue(MemoryAllocator memoryAllocator, long nullValue) {
        this(memoryAllocator, Integer.MAX_VALUE, nullValue);
    }

    private long newNode(final long e) {
        long address = malloc.allocate(NODE_SIZE);
        AMEM.putLong(null, address, e);
        AMEM.putLong(null, address + NEXT_OFFSET, NULL_PTR);
        return address;
    }

    private static long getItem(long node) {
        assert node != NULL_PTR;
        return AMEM.getLong(node);
    }

    private static long getNextNode(long node) {
        assert node != NULL_PTR;
        return AMEM.getLong(node + NEXT_OFFSET);
    }

    private static void setNextNode(long node, long value) {
        assert node != NULL_PTR;
        AMEM.putLong(node + NEXT_OFFSET, value);
    }

    @Override
    public boolean offer(long value) {
        if (tail == NULL_PTR) {
            throw new IllegalStateException("Queue is already destroyed! " + toString());
        }
        if (value == nullItem) {
            throw new IllegalArgumentException();
        }
        if (hasCapacity && size == capacity) {
            return false;
        }
        long node = newNode(value);
        setNextNode(tail, node);
        tail = node;
        size++;
        return true;
    }

    @Override
    public long poll() {
        ensureMemory();
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

    @Override
    public long peek() {
        ensureMemory();
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

    @Override
    public int size() {
        long c = size;
        return c < Integer.MAX_VALUE ? (int) c : Integer.MAX_VALUE;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public int remainingCapacity() {
        return hasCapacity ? capacity - size() : Integer.MAX_VALUE;
    }

    @Override
    @SuppressWarnings("checkstyle:emptyblock")
    public void clear() {
        while (poll() != nullItem) { }
    }

    @Override
    public void dispose() {
        if (head != NULL_PTR) {
            clear();
            assert head == tail;
            if (head != NULL_PTR) {
                malloc.free(head, NODE_SIZE);
            }
            head = NULL_PTR;
            tail = NULL_PTR;
        }
    }

    @Override
    public long nullItem() {
        return nullItem;
    }

    @Override
    public LongIterator iterator() {
        ensureMemory();
        return new Iter();
    }

    private void ensureMemory() {
        if (head == NULL_PTR) {
            throw new IllegalStateException("Queue is already destroyed! " + toString());
        }
    }

    private final class Iter implements LongIterator {
        long currentNode;

        private Iter() {
            reset();
        }

        @Override
        public boolean hasNext() {
            return currentNode != NULL_PTR;
        }

        @Override
        public long next() {
            if (currentNode == NULL_PTR) {
                throw new NoSuchElementException();
            }
            ensureMemory();
            long item = getItem(currentNode);
            currentNode = getNextNode(currentNode);
            return item;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset() {
            currentNode = getNextNode(head);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LongLinkedQueue{");
        sb.append("nullItem=").append(nullItem);
        sb.append(", capacity=").append(capacity);
        sb.append(", head=").append(head);
        sb.append(", tail=").append(tail);
        sb.append(", size=").append(size);
        sb.append('}');
        return sb.toString();
    }
}


