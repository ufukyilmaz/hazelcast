package com.hazelcast.elastic.queue;

import com.hazelcast.elastic.LongIterator;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.nio.UnsafeHelper;
import sun.misc.Unsafe;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

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
        tail = head = newNode(nullItem);
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
                throw new IllegalStateException("Queue is already destroyed! " + toString());
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

    public int size() {
        long c = size.get();
        return c < Integer.MAX_VALUE ? (int) c : Integer.MAX_VALUE;
    }

    public boolean isEmpty() {
        return size.get() == 0;
    }

    public int capacity() {
        return capacity;
    }

    public int remainingCapacity() {
        return hasCapacity ? capacity - size() : Integer.MAX_VALUE ;
    }

    public void clear() {
        fullLock();
        try {
            while (poll() != nullItem);
        } finally {
            fullUnlock();
        }
    }

    public void dispose() {
        fullLock();
        try {
            if (head != NULL_PTR) {
                clear();
                long ptr = head;
                long ptr2 = tail;
                head = tail = NULL_PTR;
                assert ptr == ptr2;
                if (ptr != NULL_PTR) {
                    malloc.free(ptr, NODE_SIZE);
                }
            }
        } finally {
            fullUnlock();
        }
    }

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
        final StringBuilder sb = new StringBuilder("LongLinkedBlockingQueue{");
        sb.append("capacity=").append(capacity);
        sb.append(", size=").append(size.get());
        sb.append('}');
        return sb.toString();
    }
}
