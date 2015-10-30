package com.hazelcast.elastic.queue;

import com.hazelcast.elastic.LongIterator;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.nio.UnsafeHelper;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public final class LongArrayBlockingQueue implements LongBlockingQueue {
    private final MemoryAllocator malloc;
    private final int capacity;
    private final long address;
    private final long nullItem;
    private int add;
    private int remove;
    private int size;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();

    public LongArrayBlockingQueue(final MemoryAllocator memoryAllocator, int capacity, long nullItem) {
        this.malloc = memoryAllocator;
        this.capacity = capacity;
        this.nullItem = nullItem;
        long realCap = capacity * 8L;
        this.address = malloc.allocate(realCap);
        clear();
    }

    private long get(int index) {
        if (index >= capacity || index < 0) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        return UnsafeHelper.UNSAFE.getLong(address + (index * 8L));
    }

    private void set(int index, long value) {
        if (index >= capacity || index < 0) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        UnsafeHelper.UNSAFE.putLong(address + (index * 8L), value);
    }

    public boolean offer(long value) {
        if (value == nullItem) {
            throw new IllegalArgumentException();
        }

        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (size == capacity) {
                return false;
            }
            return insert(value);
        } finally {
            lock.unlock();
        }
    }

    public boolean offer(long value, long timeout, TimeUnit unit) throws InterruptedException {
        if (value == nullItem) {
            throw new IllegalArgumentException();
        }

        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            for (;;) {
                if (size < capacity) {
                    return insert(value);
                }
                if (nanos <= 0)
                    return false;
                try {
                    nanos = notFull.awaitNanos(nanos);
                } catch (InterruptedException ie) {
                    notFull.signal();
                    throw ie;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void put(long value) throws InterruptedException {
        if (value == nullItem) {
            throw new IllegalArgumentException();
        }

        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            try {
                while (size == capacity) {
                    notFull.await();
                }
            } catch (InterruptedException ie) {
                notFull.signal();
                throw ie;
            }
            insert(value);
        } finally {
            lock.unlock();
        }
    }

    private boolean insert(final long value) {
        ensureMemory();
        set(add, value);
        add++;
        size++;
        if (add == capacity) {
            add = 0;
        }
        notEmpty.signal();
        return true;
    }

    public long peek() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            ensureMemory();
            if (size == 0) {
                return nullItem;
            }
            return get(remove);
        } finally {
            lock.unlock();
        }
    }

    public long poll() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (size == 0) {
                return nullItem;
            }
            return extract();
        } finally {
            lock.unlock();
        }
    }

    public long poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            for (;;) {
                if (size != 0) {
                    return extract();
                }
                if (nanos <= 0)
                    return nullItem;
                try {
                    nanos = notEmpty.awaitNanos(nanos);
                } catch (InterruptedException ie) {
                    notEmpty.signal();
                    throw ie;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public long take() throws InterruptedException {
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            try {
                while (size == 0) {
                    notEmpty.await();
                }
            } catch (InterruptedException ie) {
                notEmpty.signal();
                throw ie;
            }
            return extract();
        } finally {
            lock.unlock();
        }
    }

    private long extract() {
        ensureMemory();
        long value = get(remove);
        set(remove, nullItem);
        remove++;
        size--;
        if (remove == capacity) {
            remove = 0;
        }
        notFull.signal();
        return value;
    }

    private void ensureMemory() {
        if (size < 0) {
            throw new IllegalStateException("Queue is already destroyed! " + toString());
        }
    }

    public void consume(final LongConsumer consumer) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            while (size > 0) {
                long value = get(remove);
                if (consumer.consume(value)) {
                    set(remove, nullItem);
                    remove++;
                    size--;
                    if (remove == capacity) {
                        remove = 0;
                    }
                }
            }
            notFull.signal();
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return size;
        } finally {
            lock.unlock();
        }
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public int capacity() {
        return capacity;
    }

    public int remainingCapacity() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return capacity - size;
        } finally {
            lock.unlock();
        }
    }

    public void clear() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            UnsafeHelper.UNSAFE.setMemory(address, capacity * 8L, (byte) 0);
            add = 0;
            remove = 0;
            size = 0;
        } finally {
            lock.unlock();
        }
    }

    public void dispose() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (size > -1) {
                malloc.free(address, capacity * 8L);
                add = 0;
                remove = 0;
                size = -1;
            }
        } finally {
            lock.unlock();
        }
    }

    public long nullItem() {
        return nullItem;
    }

    public LongIterator iterator() {
        lock.lock();
        try {
            ensureMemory();
            return new Iter();
        } finally {
            lock.unlock();
        }
    }

    // TODO: broken, currently does not reflect external changes!
    private class Iter implements LongIterator {
        // hold nextItem to return if hasNext() returns true
        private int remaining;
        private int nextIndex;
        private long nextItem;

        // created under lock
        Iter() {
            reset();
        }

        public boolean hasNext() {
            return remaining > 0;
        }

        public long next() {
            if (remaining <= 0) {
                throw new NoSuchElementException();
            }

            final ReentrantLock lock = LongArrayBlockingQueue.this.lock;
            lock.lock();
            try {
                ensureMemory();
                long item = nextItem;
                while (--remaining > 0) {
                    if ((nextItem = get(nextIndex = inc(nextIndex))) != nullItem) break;
                }
                return item;
            } finally {
                lock.unlock();
            }
        }

        private int inc(int i) {
            return (++i == capacity)? 0 : i;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset() {
            if ((remaining = size) > 0) nextItem = get(nextIndex = remove);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LongArrayBlockingQueue{");
        sb.append("capacity=").append(capacity);
        sb.append(", address=").append(address);
        sb.append(", size=").append(size());
        sb.append('}');
        return sb.toString();
    }
}
