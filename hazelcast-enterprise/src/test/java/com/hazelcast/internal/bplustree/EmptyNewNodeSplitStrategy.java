package com.hazelcast.internal.bplustree;


public class EmptyNewNodeSplitStrategy implements NodeSplitStrategy {

    @Override
    public int getNewNodeKeysCount(int keysCount) {
        return 0;
    }
}
