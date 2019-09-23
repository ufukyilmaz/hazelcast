package com.hazelcast.internal.elastic.tree;

import com.hazelcast.internal.memory.MemoryBlock;

/**
 * Comparator of the off-heap values given to the compare methods as native addresses and sizes.
 */
public interface OffHeapComparator {
    /***
     * Compares two blobs specified by addresses and sizes;
     *
     * @param left  The left blob;
     * @param right The right blob;
     * @return
     *          1 if left blob greater than right blob;
     *          -1 if right blob greater than left blob;
     *          0 if blobs are equal;
     */
    int compare(MemoryBlock left, MemoryBlock right);

    /***
     * Compares two byte array blobs;
     *
     * @param left  The left blob;
     * @param right The right blob;
     * @return
     *          1 if left blob greater than right blob;
     *          -1 if right blob greater than left blob;
     *          0 if blobs are equal;
     */
    int compare(byte[] left, byte[] right);
}
