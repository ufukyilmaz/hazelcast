package com.hazelcast.elastic.tree;

/**
 * Comparator of the off-heap values given to the compare methods as native addresses and sizes.
 */
public interface OffHeapComparator {
    /***
     * Compares to blobs specified by addresses and sizes;
     *
     * @param leftAddress  address of the first blob;
     * @param leftSize     size of the first blob;
     * @param rightAddress address of the second blob;
     * @param rightSize    size of the second blob;
     * @return
     *          1 if left blob greater than right blob;
     *          -1 if right blob greater than left blob;
     *          0 if blobs are equal;
     */
    int compare(long leftAddress, long leftSize,
                long rightAddress, long rightSize);
}
