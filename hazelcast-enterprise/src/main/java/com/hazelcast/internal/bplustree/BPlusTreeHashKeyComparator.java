package com.hazelcast.internal.bplustree;

public final class BPlusTreeHashKeyComparator implements BPlusTreeKeyComparator {

    @Override
    public int compare(Comparable left, long rightAddress) {
        // Right key is never considered as "null"

        assert left == null || left instanceof Long : left;

        if (left != null) {
            return left.compareTo(rightAddress);
        } else {
            return -1;
        }
    }
}
