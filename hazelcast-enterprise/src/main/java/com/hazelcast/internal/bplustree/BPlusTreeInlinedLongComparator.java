package com.hazelcast.internal.bplustree;

/**
 * The B+tree keys comparator interpreting the index key address as inlined long value.
 */
public final class BPlusTreeInlinedLongComparator implements BPlusTreeKeyComparator {

    @Override
    public int compare(Comparable left, long rightAddress, long rightPayload) {
        // Right key is never considered as "null"

        assert left == null || left instanceof Long : left;

        if (left != null) {
            return left.compareTo(rightAddress);
        } else {
            return -1;
        }
    }
}
