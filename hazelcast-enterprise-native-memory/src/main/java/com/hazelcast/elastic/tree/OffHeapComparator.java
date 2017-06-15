package com.hazelcast.elastic.tree;

import com.hazelcast.memory.MemoryBlock;

/**
 * Comparator of the off-heap values given to the compare methods as native addresses and sizes.
 */
public interface OffHeapComparator {
    /***
     * Compares to blobs specified by addresses and sizes;
     *
     * @param left  The left blob;
     * @param right The right blob;
     * @return
     *          1 if left blob greater than right blob;
     *          -1 if right blob greater than left blob;
     *          0 if blobs are equal;
     */
    int compare(MemoryBlock left, MemoryBlock right);
}
