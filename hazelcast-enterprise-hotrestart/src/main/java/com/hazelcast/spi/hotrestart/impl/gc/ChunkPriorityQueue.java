/*
 * Derived from Apache Lucene's PriorityQueue
 *
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.hotrestart.impl.gc;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** A PriorityQueue maintains a partial ordering of its elements such that the
 * worst element can always be found in constant time.  Put()'s and pop()'s
 * require log(size) time.
 */
class ChunkPriorityQueue {
    private int size;
    private final int maxSize;
    private final StableChunk[] heap;

    ChunkPriorityQueue(int maxSize) {
        this.heap = new StableChunk[maxSize == 0 ? 2 : maxSize + 1];
        this.maxSize = maxSize;
    }

    private static boolean betterThan(StableChunk left, StableChunk right) {
        return left.cachedCostBenefit() > right.cachedCostBenefit();
    }

    /**
     * Adds an Object to a PriorityQueue in log(size) time.
     * It returns the object (if any) that was
     * dropped off the heap because it was full. This can be
     * the given parameter (in case it isn't better than the
     * full heap's minimum, and couldn't be added), or another
     * object that was previously the worst value in the
     * heap and now has been replaced by a better one, or null
     * if the queue wasn't yet full with maxSize elements.
     */
    public StableChunk consider(StableChunk element) {
        if (size < maxSize) {
            size++;
            heap[size] = element;
            upHeap();
            return null;
        } else if (size > 0 && betterThan(element, heap[1])) {
            StableChunk ret = heap[1];
            heap[1] = element;
            downHeap();
            return ret;
        } else {
            return element;
        }
    }

    public StableChunk head() {
        return size > 0 ? heap[1] : null;
    }

    /** Removes and returns the least element of the PriorityQueue in log(size)
     time. */
    public StableChunk pop() {
        if (size > 0) {
            StableChunk result = heap[1];
            heap[1] = heap[size];
            size--;
            downHeap();
            return result;
        } else {
            return null;
        }
    }

    public int size() {
        return size;
    }

    public void clear() {
        size = 0;
    }

    public List<StableChunk> asList() {
        return isEmpty() ? Collections.<StableChunk>emptyList() : Arrays.asList(heap).subList(1, size + 1);
    }

    public boolean isEmpty() {
        return size == 0;
    }

    private void upHeap() {
        int i = size;
        // save bottom node
        StableChunk node = heap[i];
        int j = i >>> 1;
        while (j > 0 && betterThan(heap[j], node)) {
            // shift parents down
            heap[i] = heap[j];
            i = j;
            j >>>= 1;
        }
        // install saved node
        heap[i] = node;
    }

    private void downHeap() {
        int i = 1;
        // save top node
        StableChunk node = heap[i];
        // find worse child
        int j = i << 1;
        int k = j + 1;
        if (k <= size && betterThan(heap[j], heap[k])) {
            j = k;
        }
        while (j <= size && betterThan(node, heap[j])) {
            // shift up child
            heap[i] = heap[j];
            i = j;
            j = i << 1;
            k = j + 1;
            if (k <= size && betterThan(heap[j], heap[k])) {
                j = k;
            }
        }
        // install saved node
        heap[i] = node;
    }
}
