/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.elasticcollections.queue;

import com.hazelcast.elasticcollections.LongIterator;
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

    public boolean offer(long value) {
        assert value != nullItem;
        if (size < 0) {
            throw new IllegalStateException("Queue is already destroyed! " + toString());
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

    public long peek() {
        if (size < 0) {
            throw new IllegalStateException("Queue is already destroyed! " + toString());
        }
        if (size == 0) {
            return nullItem;
        }
        return get(remove);
    }

    public long poll() {
        if (size < 0) {
            throw new IllegalStateException("Queue is already destroyed! " + toString());
        }
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

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public int capacity() {
        return capacity;
    }

    public int remainingCapacity() {
        return capacity - size;
    }

    public void clear() {
        if (size < 0) {
            throw new IllegalStateException("Queue is already destroyed! " + toString());
        }
        for (int i = 0; i < capacity; i++) {
            UnsafeHelper.UNSAFE.putLong(address + (i * 8L), nullItem);
        }
        add = 0;
        remove = 0;
        size = 0;
    }

    public void destroy() {
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

    public LongIterator iterator() {
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
            long item = get(cursor);
            cursor = inc(cursor);
            remaining--;
            return item;
        }

        private int inc(int i) {
            return (++i == capacity) ? 0 : i;
        }

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
