package com.hazelcast.internal.elastic.queue;

import com.hazelcast.internal.elastic.LongIterator;
import com.hazelcast.internal.memory.MemoryAllocator;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;

/** Implementation of {@link LongBlockingQueue} with a linked list. */
public final class LongLinkedBlockingQueue implements LongBlockingQueue {

    private static final long NULL_PTR = 0L;
    private static final int NODE_SIZE = 16;
    private static final int NEXT_OFFSET = 8;

    private final MemoryAllocator malloc;

    private final long nullItem;

    private final int capacity;

    private final boolean hasCapacity;

    private long head;

    private long tail;

    private final AtomicLong size = new AtomicLong();

    private final ReentrantLock headLock = new ReentrantLock();

    private final Condition notEmpty = headLock.newCondition();

    private final ReentrantLock tailLock = new ReentrantLock();

    private final Condition notFull = tailLock.newCondition();

    public LongLinkedBlockingQueue(final MemoryAllocator malloc, final long nullValue) {
        this(malloc, Integer.MAX_VALUE, nullValue);
    }

    public LongLinkedBlockingQueue(final MemoryAllocator malloc, final int capacity, final long nullValue) {
        this.malloc = malloc;
        this.capacity = capacity;
        this.hasCapacity =  capacity < Integer.MAX_VALUE;
        nullItem = nullValue;
        head = newNode(nullItem);
        tail = head;
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
    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean offer(long value) {
        if (value == nullItem) {
            throw new IllegalArgumentException();
        }

        if (hasCapacity && size.get() == capacity) {
            return false;
        }

        long c = -1;
        tailLock.lock();
        try {
            if (tail == NULL_PTR) {
                throw new IllegalStateException("Queue is already disposed! " + toString());
            }
            if (!hasCapacity || size.get() < capacity) {
                long node = newNode(value);
                setNextNode(tail, node);
                tail = node;

                c = size.getAndIncrement();
                if (c + 1 < capacity) {
                    notFull.signal();
                }
            }
        } finally {
            tailLock.unlock();
        }
        if (c == 0) {
            signalNotEmpty();
        }
        return c >= 0;
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean offer(long value, long timeout, TimeUnit unit) throws InterruptedException {
        if (value == nullItem) {
            throw new IllegalArgumentException();
        }

        long nanos = unit.toNanos(timeout);
        long c = -1;
        tailLock.lock();
        try {
            if (tail == NULL_PTR) {
                throw new IllegalStateException("Queue is already destroyed! " + toString());
            }
            if (hasCapacity) {
                while (size.get() == capacity) {
                    if (nanos <= 0) {
                        return false;
                    }
                    nanos = notFull.awaitNanos(nanos);
                }
            }

            long node = newNode(value);
            setNextNode(tail, node);
            tail = node;

            c = size.getAndIncrement();
            if (c + 1 < capacity) {
                notFull.signal();
            }
        } finally {
            tailLock.unlock();
        }
        if (c == 0) {
            signalNotEmpty();
        }
        return c >= 0;
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public void put(long value) throws InterruptedException {
        if (value == nullItem) {
            throw new IllegalArgumentException();
        }

        long c = -1;
        tailLock.lock();
        try {
            if (tail == NULL_PTR) {
                throw new IllegalStateException("Queue is already destroyed! " + toString());
            }
            if (hasCapacity) {
                while (size.get() == capacity) {
                    notFull.await();
                }
            }

            long node = newNode(value);
            setNextNode(tail, node);
            tail = node;

            c = size.getAndIncrement();
            if (c + 1 < capacity) {
                notFull.signal();
            }
        } finally {
            tailLock.unlock();
        }
        if (c == 0) {
            signalNotEmpty();
        }
    }

    @Override
    public long poll() {
//        if (size.get() == 0) {
//            return nullItem;
//        }

        long item;
        long node;
        long c = -1;
        headLock.lock();
        try {
            if (head == NULL_PTR) {
                throw new IllegalStateException("Queue is already destroyed! " + toString());
            }
            node = head;
            long newHead = getNextNode(node);
            if (newHead == NULL_PTR) {
                return nullItem;
            }
            item = getItem(newHead);
            head = newHead;

            c = size.getAndDecrement();
            if (c > 1) {
                notEmpty.signal();
            }
        } finally {
            headLock.unlock();
        }
        malloc.free(node, NODE_SIZE);
        if (c == capacity) {
            signalNotFull();
        }
        return item;
    }

    @Override
    public long poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        long item;
        long node;
        long c = -1;
        headLock.lockInterruptibly();
        try {
            if (head == NULL_PTR) {
                throw new IllegalStateException("Queue is already destroyed! " + toString());
            }
            while (size.get() == 0) {
                if (nanos <= 0) {
                    return nullItem;
                }
                nanos = notEmpty.awaitNanos(nanos);
            }
            node = head;
            long newHead = getNextNode(node);
            item = getItem(newHead);
            head = newHead;

            c = size.getAndDecrement();
            if (c > 1) {
                notEmpty.signal();
            }
        } finally {
            headLock.unlock();
        }
        malloc.free(node, NODE_SIZE);
        if (c == capacity) {
            signalNotFull();
        }
        return item;
    }

    @Override
    public long take() throws InterruptedException {
        long item;
        long node;
        long c = -1;
        headLock.lockInterruptibly();
        try {
            if (head == NULL_PTR) {
                throw new IllegalStateException("Queue is already destroyed! " + toString());
            }
            while (size.get() == 0) {
                notEmpty.await();
            }
            node = head;
            long newHead = getNextNode(node);
            item = getItem(newHead);
            head = newHead;

            c = size.getAndDecrement();
            if (c > 1) {
                notEmpty.signal();
            }
        } finally {
            headLock.unlock();
        }
        malloc.free(node, NODE_SIZE);
        if (c == capacity) {
            signalNotFull();
        }
        return item;
    }

    @Override
    public void consume(final LongConsumer consumer) {
        final ReentrantLock lock = this.headLock;
        lock.lock();
        try {
            long c = size.get();
            while (c > 0) {
                long node = head;
                long newHead = getNextNode(node);
                long item = getItem(newHead);
                if (consumer.consume(item)) {
                    head = newHead;
                    c = size.decrementAndGet();
                    malloc.free(node, NODE_SIZE);
                } else {
                    break;
                }
            }
            if (c > 0) {
                notEmpty.signal();
            }
        } finally {
            lock.unlock();
        }
        signalNotFull();
    }

    @Override
    public long peek() {
        if (size.get() == 0) {
            return nullItem;
        }
        headLock.lock();
        try {
            if (head == NULL_PTR) {
                throw new IllegalStateException("Queue is already destroyed! " + toString());
            }
            long first = getNextNode(head);
            if (first == NULL_PTR) {
                return nullItem;
            } else {
                return getItem(first);
            }
        } finally {
            headLock.unlock();
        }
    }

    @Override
    public int size() {
        long c = size.get();
        return c < Integer.MAX_VALUE ? (int) c : Integer.MAX_VALUE;
    }

    @Override
    public boolean isEmpty() {
        return size.get() == 0;
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
        fullLock();
        try {
            while (poll() != nullItem) { }
        } finally {
            fullUnlock();
        }
    }

    @Override
    public void dispose() {
        fullLock();
        try {
            if (head != NULL_PTR) {
                clear();
                long headPtr = head;
                long tailPtr = tail;
                head = NULL_PTR;
                tail = NULL_PTR;
                assert headPtr == tailPtr;
                if (headPtr != NULL_PTR) {
                    malloc.free(headPtr, NODE_SIZE);
                }
            }
        } finally {
            fullUnlock();
        }
    }

    @Override
    public long nullItem() {
        return nullItem;
    }

    public LongIterator iterator() {
        throw new UnsupportedOperationException();
    }

    private void signalNotEmpty() {
        headLock.lock();
        try {
            notEmpty.signal();
        } finally {
            headLock.unlock();
        }
    }

    private void signalNotFull() {
        tailLock.lock();
        try {
            notFull.signal();
        } finally {
            tailLock.unlock();
        }
    }

    private void fullUnlock() {
        tailLock.unlock();
        headLock.unlock();
    }

    private void fullLock() {
        headLock.lock();
        tailLock.lock();
    }

    @Override
    public String toString() {
        return "LongLinkedBlockingQueue{capacity=" + capacity + ", size=" + size.get() + '}';
    }
}
