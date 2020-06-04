package com.hazelcast.internal.bplustree;

/**
 * The lock fairness policy to customize Lock Manager behavior.
 */
public interface LockFairnessPolicy {

    /**
     * Returns percentage in range [0.0, 100.0], so that the write waiters awaiting on condition
     * will be signalled to awake with this probability. Otherwise, read waiters will be signalled.
     * <p>
     * The value greater 50.0 favors writer lock requests.
     */
    float writeWaiterWinsPercentage();

    /**
     * Returns percentage in range [0.0, 100.0], so that the read lock request is allowed to grab the
     * lock if it is compatible with the request while there are existing waiters for the lock.
     * <p>
     * The value 100.0 favors read lock requests throughput over fairness risking waiting requests starvation.
     */
    float readLockRequestWinsPercentage();

    /**
     * Returns percentage in range [0.0, 100.0], so that the write lock request is allowed to grab the
     * lock if it is compatible with the request while there are existing waiters for the lock.
     * <p>
     * The value 100.0 favors write lock requests throughput over fairness risking waiting requests starvation.
     */
    float writeLockRequestWinsPercentage();
}
