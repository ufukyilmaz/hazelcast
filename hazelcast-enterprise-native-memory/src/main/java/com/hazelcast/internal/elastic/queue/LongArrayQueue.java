package com.hazelcast.internal.elastic.queue;

import com.hazelcast.internal.elastic.LongIterator;
import com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry;
import com.hazelcast.internal.memory.GlobalMemoryAccessorType;
import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryAllocator;

import java.util.NoSuchElementException;

/** Implementation of {@link LongQueue} with an off-heap array. */
public final class LongArrayQueue implements LongQueue {

    static final long ENTRY_SIZE = 8L;

    // We are using `STANDARD` memory accessor because we internally guarantee that
    // every memory access is aligned.
    private static final MemoryAccessor MEMORY_ACCESSOR =
            GlobalMemoryAccessorRegistry.getGlobalMemoryAccessor(GlobalMemoryAccessorType.STANDARD);

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
        long realCap = capacity * ENTRY_SIZE;
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
        return MEMORY_ACCESSOR.getLong(address + (index * ENTRY_SIZE));
    }

    private void set(int index, long value) {
        if (index >= capacity || index < 0) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        MEMORY_ACCESSOR.putLong(address + (index * ENTRY_SIZE), value);
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
            MEMORY_ACCESSOR.putLong(address + (i * ENTRY_SIZE), nullItem);
        }
        add = 0;
        remove = 0;
        size = 0;
    }

    @Override
    public void dispose() {
        if (size >= 0) {
            malloc.free(address, capacity * ENTRY_SIZE);
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

    public boolean isAvailable() {
        return size >= 0;
    }

    private final class Iter implements LongIterator {
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
        return "LongArrayQueue{capacity=" + capacity + ", size=" + size + ", add="
                + add + ", remove=" + remove + ", nullItem=" + nullItem + '}';
    }
}
