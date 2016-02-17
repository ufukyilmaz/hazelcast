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

import com.hazelcast.util.function.Consumer;

import java.util.Collection;

import static com.hazelcast.internal.memory.MemoryAccessor.MEM;

/**
 * Many producer to one consumer concurrent queue that is array backed. The algorithm is a variation of Fast Flow
 * consumer
 * adapted to work with the Java Memory Model on arrays by using {@link sun.misc.Unsafe}.
 *
 * @param <E> type of the elements stored in the {@link java.util.Queue}.
 */
public class ManyToOneConcurrentArrayQueue<E> extends AbstractConcurrentArrayQueue<E> {
    public ManyToOneConcurrentArrayQueue(final int requestedCapacity) {
        super(requestedCapacity);
    }

    public boolean offer(final E e) {
        if (null == e) {
            throw new NullPointerException("element cannot be null");
        }

        long currentTail;
        final long bufferLimit = head + capacity;
        do {
            currentTail = tail;

            if (currentTail >= bufferLimit) {
                return false;
            }
        } while (!MEM.compareAndSwapLong(this, TAIL_OFFSET, currentTail, currentTail + 1));

        MEM.putOrderedObject(buffer, sequenceToOffset(currentTail, mask), e);

        return true;
    }

    @SuppressWarnings("unchecked")
    public E poll() {
        final long currentHead = head;
        final long elementOffset = sequenceToOffset(currentHead, mask);
        final Object[] buffer = this.buffer;
        final E item = (E) MEM.getObjectVolatile(buffer, elementOffset);

        if (null != item) {
            MEM.putObject(buffer, elementOffset, null);
            MEM.putOrderedLong(this, HEAD_OFFSET, currentHead + 1);
        }

        return item;
    }

    @SuppressWarnings("unchecked")
    @Override public int drain(final Consumer<E> elementHandler) {
        final Object[] buffer = this.buffer;
        final long mask = this.mask;
        final long currentHead = head;
        long nextSequence = currentHead;

        try {
            do {
                final long elementOffset = sequenceToOffset(nextSequence, mask);
                final Object item = MEM.getObjectVolatile(buffer, elementOffset);
                if (null == item) {
                    break;
                }

                MEM.putObject(buffer, elementOffset, null);
                nextSequence++;
                elementHandler.accept((E) item);
            } while (true);
        } finally {
            MEM.putOrderedLong(this, HEAD_OFFSET, nextSequence);
        }

        return (int) (nextSequence - currentHead);
    }

    @SuppressWarnings("unchecked")
    @Override public int drainTo(final Collection<? super E> target, final int limit) {
        final Object[] buffer = this.buffer;
        final long mask = this.mask;
        long nextSequence = head;
        int count = 0;

        while (count < limit) {
            final long elementOffset = sequenceToOffset(nextSequence, mask);
            final Object item = MEM.getObjectVolatile(buffer, elementOffset);
            if (null == item) {
                break;
            }

            MEM.putObject(buffer, elementOffset, null);
            nextSequence++;
            count++;
            target.add((E) item);
        }

        MEM.putOrderedLong(this, HEAD_OFFSET, nextSequence);

        return count;
    }
}
