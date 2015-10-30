package com.hazelcast.elastic.queue;

import com.hazelcast.elastic.LongIterator;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.nio.UnsafeHelper;

import java.util.NoSuchElementException;

public final class LongArrayQueue implements LongQueue {
    private final MemoryAllocator malloc;
    private final int capacity;
    private final long address;
    private final long nullItem;
    private int add;
    private int remove;
    private int size;

    public LongArrayQueue(final MemoryAllocator memoryAllocator, int capacity, long nullItem) {
        this.malloc = memoryAllocator;
        this.capacity = capacity;
        this.nullItem = nullItem;
        long realCap = capacity * 8L;
        this.address = malloc.allocate(realCap);
        clear();
    }

    public LongArrayQueue(final MemoryAllocator malloc, LongQueue queue) {
        this(malloc, queue.size(), queue);
    }

    public LongArrayQueue(final MemoryAllocator malloc, int capacity, LongQueue queue) {
        this(malloc, capacity, queue.nullItem());
        long value;
        while ((value = queue.poll()) != nullItem) {
            if (!offer(value)) {
                throw new IllegalStateException("This: " + toString() + ", That: " + queue);
            }
        }
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

    @Override
    public boolean offer(long value) {
        ensureMemory();
        if (value == nullItem) {
            throw new IllegalArgumentException();
        }
        if (size == capacity) {
            return false;
        }
        set(add, value);
        add++;
        size++;
        if (add == capacity) {
            add = 0;
        }
        return true;
    }

    @Override
    public long peek() {
        ensureMemory();
        if (size == 0) {
            return nullItem;
        }
        return get(remove);
    }

    @Override
    public long poll() {
        ensureMemory();
        if (size == 0) {
            return nullItem;
        }
        long value = get(remove);
        set(remove, nullItem);
        remove++;
        size--;
        if (remove == capacity) {
            remove = 0;
        }
        return value;
    }

    private void ensureMemory() {
        if (size < 0) {
            throw new IllegalStateException("Queue is already destroyed! " + toString());
        }
    }

    @Override
    public int size() {
        return size;
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
        return capacity - size;
    }

    @Override
    public void clear() {
        ensureMemory();
        for (int i = 0; i < capacity; i++) {
            UnsafeHelper.UNSAFE.putLong(address + (i * 8L), nullItem);
        }
        add = 0;
        remove = 0;
        size = 0;
    }

    @Override
    public void dispose() {
        if (size >= 0) {
            malloc.free(address, capacity * 8L);
            add = 0;
            remove = 0;
            size = -1;
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

    private class Iter implements LongIterator {
        int remaining;
        int cursor;

        private Iter() {
            reset();
        }

        @Override
        public boolean hasNext() {
            return remaining > 0;
        }

        @Override
        public long next() {
            if (remaining == 0) {
                throw new NoSuchElementException();
            }
            ensureMemory();
            long item = get(cursor);
            cursor = inc(cursor);
            remaining--;
            return item;
        }

        private int inc(int i) {
            return (++i == capacity) ? 0 : i;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset() {
            remaining = size();
            cursor = remove;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LongArrayQueue{");
        sb.append("capacity=").append(capacity);
        sb.append(", size=").append(size);
        sb.append(", add=").append(add);
        sb.append(", remove=").append(remove);
        sb.append(", nullItem=").append(nullItem);
        sb.append('}');
        return sb.toString();
    }
}
