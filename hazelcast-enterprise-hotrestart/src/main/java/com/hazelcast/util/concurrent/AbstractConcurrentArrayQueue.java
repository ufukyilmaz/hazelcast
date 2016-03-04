/*
 * Original work Copyright 2015 Real Logic Ltd.
 * Modified work Copyright (c) 2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.util.concurrent;

import com.hazelcast.util.QuickMath;
import com.hazelcast.util.function.Consumer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_OBJECT_BASE_OFFSET;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_OBJECT_INDEX_SCALE;

/**
 * Pad out a cacheline to the left of a tail to prevent false sharing.
 */
@SuppressFBWarnings(value = "UuF", justification = "Fields used for padding are unused programatically")
class Padding1 {
    @SuppressWarnings("unused")
    protected long p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15;
}

/**
 * Value for the tail that is expected to be padded.
 */
@SuppressFBWarnings(value = "UuF", justification = "Fields used for padding are unused programatically")
class Tail extends Padding1 {
    protected volatile long tail;
}

/**
 * Pad out a cacheline between the tail and the head to prevent false sharing.
 */
@SuppressFBWarnings(value = "UuF", justification = "Fields used for padding are unused programatically")
class Padding2 extends Tail {
    @SuppressWarnings("unused")
    protected long p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30;
}

/**
 * Value for the head that is expected to be padded.
 */
@SuppressFBWarnings(value = "UuF", justification = "Fields used for padding are unused programatically")
class Head extends Padding2 {
    protected volatile long head;
}

/**
 * Pad out a cacheline between the tail and the head to prevent false sharing.
 */
@SuppressFBWarnings(value = "UuF", justification = "Fields used for padding are unused programatically")
class Padding3 extends Head {
    @SuppressWarnings("unused")
    protected long p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45;
}

/**
 * Abstract base class for concurrent array queues.
 * @param <E> type of elements in the queue.
 */
@SuppressFBWarnings(value = "UuF", justification = "Fields used for padding are unused programatically")
public abstract class AbstractConcurrentArrayQueue<E> extends Padding3 implements java.util.Queue<E> {
    protected static final long TAIL_OFFSET;
    protected static final long HEAD_OFFSET;
    protected static final int SHIFT_FOR_SCALE;

    static {
        try {
            SHIFT_FOR_SCALE = calculateShiftForScale(ARRAY_OBJECT_INDEX_SCALE);
            TAIL_OFFSET = AMEM.objectFieldOffset(Tail.class.getDeclaredField("tail"));
            HEAD_OFFSET = AMEM.objectFieldOffset(Head.class.getDeclaredField("head"));
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected final long mask;
    protected final int capacity;
    protected final E[] buffer;

    @SuppressWarnings("unchecked")
    public AbstractConcurrentArrayQueue(final int requestedCapacity) {
        capacity = QuickMath.nextPowerOfTwo(requestedCapacity);
        mask = capacity - 1;
        buffer = (E[]) new Object[capacity];
    }

    public abstract int drainTo(final Collection<? super E> target, final int limit);

    public abstract int drain(final Consumer<E> elementHandler);

    public long addedCount() {
        return tail;
    }

    public long removedCount() {
        return head;
    }

    public int capacity() {
        return capacity;
    }

    public int remainingCapacity() {
        return capacity() - size();
    }

    @SuppressWarnings("unchecked")
    public E peek() {
        return (E) AMEM.getObjectVolatile(buffer, sequenceToOffset(head, mask));
    }

    public boolean add(final E e) {
        if (offer(e)) {
            return true;
        }

        throw new IllegalStateException("Queue is full");
    }

    public E remove() {
        final E e = poll();
        if (null == e) {
            throw new NoSuchElementException("Queue is empty");
        }

        return e;
    }

    public E element() {
        final E e = peek();
        if (null == e) {
            throw new NoSuchElementException("Queue is empty");
        }

        return e;
    }

    public boolean isEmpty() {
        return tail == head;
    }

    public boolean contains(final Object o) {
        if (null == o) {
            return false;
        }

        final Object[] buffer = this.buffer;

        for (long i = head, limit = tail; i < limit; i++) {
            final Object e = AMEM.getObjectVolatile(buffer, sequenceToOffset(i, mask));
            if (o.equals(e)) {
                return true;
            }
        }

        return false;
    }

    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    public <T> T[] toArray(final T[] a) {
        throw new UnsupportedOperationException();
    }

    public boolean remove(final Object o) {
        throw new UnsupportedOperationException();
    }

    public boolean containsAll(final Collection<?> c) {
        for (final Object o : c) {
            if (!contains(o)) {
                return false;
            }
        }

        return true;
    }

    public boolean addAll(final Collection<? extends E> c) {
        for (final E e : c) {
            add(e);
        }

        return true;
    }

    public boolean removeAll(final Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    public boolean retainAll(final Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    public void clear() {
        Object value;
        do {
            value = poll();
        } while (null != value);
    }

    public int size() {
        long currentHeadBefore;
        long currentTail;
        long currentHeadAfter = head;

        do {
            currentHeadBefore = currentHeadAfter;
            currentTail = tail;
            currentHeadAfter = head;

        } while (currentHeadAfter != currentHeadBefore);

        return (int) (currentTail - currentHeadAfter);
    }

    public static long sequenceToOffset(final long sequence, final long mask) {
        return ARRAY_OBJECT_BASE_OFFSET + ((sequence & mask) << SHIFT_FOR_SCALE);
    }

    /**
     * Calculate the shift value to scale a number based on whether refs are compressed or not.
     *
     * @param scale of the number reported by Unsafe.
     * @return how many times the number needs to be shifted to the left.
     */
    public static int calculateShiftForScale(final int scale) {
        if (4 == scale) {
            return 2;
        } else if (8 == scale) {
            return 3;
        } else {
            throw new IllegalArgumentException("Unknown pointer size");
        }
    }
}
