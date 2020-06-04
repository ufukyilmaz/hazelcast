package com.hazelcast.internal.bplustree;

/**
 * Default lock fairness policy. It favours throughput over fairness and write waiters over read ones.
 */
public class DefaultLockFairnessPolicy implements LockFairnessPolicy {

    @SuppressWarnings("checkstyle:magicnumber")
    @Override
    public float writeWaiterWinsPercentage() {
        return 70.0f;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Override
    public float readLockRequestWinsPercentage() {
        return 100.0f;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Override
    public float writeLockRequestWinsPercentage() {
        return 100.0f;
    }
}
