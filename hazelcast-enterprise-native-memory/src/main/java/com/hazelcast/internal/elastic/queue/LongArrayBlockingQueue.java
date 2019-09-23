package com.hazelcast.internal.elastic.queue;

import com.hazelcast.internal.elastic.LongIterator;
import com.hazelcast.internal.memory.MemoryAllocator;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;


/**
 * Implementation of {@link LongBlockingQueue} with an off-heap array.
 */
public final class LongArrayBlockingQueue implements LongBlockingQueue {

    private static final long ENTRY_SIZE = 8L;

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
        long realCap = capacity * ENTRY_SIZE;
        this.address = malloc.allocate(realCap);
        clear();
    }

    private long get(int index) {
        if (index >= capacity || index < 0) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        return AMEM.getLong(address + (index * ENTRY_SIZE));
    }

    private void set(int index, long value) {
        if (index >= capacity || index < 0) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        AMEM.putLong(address + (index * ENTRY_SIZE), value);
    }

    @Override
    public boolean offer(long value) {
        if (value == nullItem) {
            throw new IllegalArgumentException();
        }

        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return size != capacity && insert(value);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(long value, long timeout, TimeUnit unit) throws InterruptedException {
        if (value == nullItem) {
            throw new IllegalArgumentException();
        }

        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            for (; ; ) {
                if (size < capacity) {
                    return insert(value);
                }
                if (nanos <= 0) {
                    return false;
                }
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

    @Override
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

    @Override
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

    @Override
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

    @Override
    public long poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            for (; ; ) {
                if (size != 0) {
                    return extract();
                }
                if (nanos <= 0) {
                    return nullItem;
                }
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

    @Override
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

    @Override
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

    @Override
    public int size() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return size;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public int remainingCapacity() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return capacity - size;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            AMEM.setMemory(address, capacity * ENTRY_SIZE, (byte) 0);
            add = 0;
            remove = 0;
            size = 0;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void dispose() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (size > -1) {
                malloc.free(address, capacity * ENTRY_SIZE);
                add = 0;
                remove = 0;
                size = -1;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long nullItem() {
        return nullItem;
    }

    @Override
    public LongIterator iterator() {
        lock.lock();
        try {
            ensureMemory();
            return new Iter();
        } finally {
            lock.unlock();
        }
    }

    // TODO broken, currently does not reflect external changes!
    private class Iter implements LongIterator {
        // hold nextItem to return if hasNext() returns true
        private int remaining;
        private int nextIndex;
        private long nextItem;

        // created under lock
        Iter() {
            reset();
        }

        @Override
        public boolean hasNext() {
            return remaining > 0;
        }

        @Override
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
                    nextIndex = inc(nextIndex);
                    nextItem = get(nextIndex);
                    if (nextItem != nullItem) {
                        break;
                    }
                }
                return item;
            } finally {
                lock.unlock();
            }
        }

        private int inc(int i) {
            int next = i + 1;
            return (next == capacity) ? 0 : next;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset() {
            remaining = size;
            if (remaining > 0) {
                nextIndex = remove;
                nextItem = get(nextIndex);
            }
        }
    }

    @Override
    public String toString() {
        return "LongArrayBlockingQueue{capacity=" + capacity + ", address=" + address + ", size=" + size() + '}';
    }
}
