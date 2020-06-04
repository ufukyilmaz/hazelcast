package com.hazelcast.internal.bplustree;

import com.hazelcast.core.HazelcastException;

/**
 * The exception is thrown when some limits of the B+tree implementation are reached.
 * That is either the depth of the tree exceeds the threshold or the number of
 * user's (waiters) of the lock exceeded a threshold, etc.
 *
 */
public final class BPlusTreeLimitException extends HazelcastException {

    public BPlusTreeLimitException(String msg) {
        super(msg);
    }

}
