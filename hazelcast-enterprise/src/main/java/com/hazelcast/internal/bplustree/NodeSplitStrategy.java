package com.hazelcast.internal.bplustree;

/**
 * The B+Tree node split strategy.
 */
public interface NodeSplitStrategy {

    /**
     * The keys count to be moved to the the new node by split operation
     * @param keysCount keys count on the node to be splitted
     * @return the keys count moved to the new node by split operation
     */
    int getNewNodeKeysCount(int keysCount);

}
