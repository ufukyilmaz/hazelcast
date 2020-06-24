package com.hazelcast.internal.bplustree;

/**
 * The default B+tree node split strategy which moves half of the slot entries to the
 * new node.
 */
public class DefaultNodeSplitStrategy implements NodeSplitStrategy {

    @Override
    public int getNewNodeKeysCount(int keysCount) {
        return keysCount - (keysCount / 2);
    }
}
