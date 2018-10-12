package com.hazelcast.enterprise.wan.replication;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class maintaining counters for elements in the WAN replication queues.
 * <p>
 * The two maintained counters:
 * <ul>
 * <li><i>primaryElementCounter</i>: counts the elements belong to primary
 * partitions</li>
 * <li><i>backupElementCounter</i>: counts the elements belong to backup
 * partitions</li>
 * </ul>
 */
class WanElementCounter {
    /**
     * Counts the elements in the WAN replication queues belong to
     * the owned partitions
     */
    private final AtomicInteger primaryElementCounter = new AtomicInteger(0);
    /**
     * Counts the elements in the WAN replication queues belong to
     * the backup partitions
     */
    private final AtomicInteger backupElementCounter = new AtomicInteger(0);

    /**
     * Increments the backup or primary event counter based on the flag
     * {@code backupEvent}
     *
     * @param backupEvent {@code true} if the backup event counter should
     *                    be incremented, otherwise the primary event
     *                    counter is incremented
     */
    void incrementCounters(boolean backupEvent) {
        if (backupEvent) {
            backupElementCounter.incrementAndGet();
        } else {
            primaryElementCounter.incrementAndGet();
        }
    }

    /**
     * Decrements the primary event counter by a delta
     *
     * @param delta The value the primary event counter to be decreased by
     */
    void decrementPrimaryElementCounter(int delta) {
        primaryElementCounter.addAndGet(-delta);
    }

    /**
     * Decrements the backup event counter by a delta
     *
     * @param delta The value the backup event counter to be decreased by
     */
    void decrementBackupElementCounter(int delta) {
        backupElementCounter.addAndGet(-delta);
    }

    /**
     * Decrements the primary event counter by one
     */
    void decrementPrimaryElementCounter() {
        primaryElementCounter.decrementAndGet();
    }

    /**
     * Decrements the backup event counter by one
     */
    void decrementBackupElementCounter() {
        backupElementCounter.decrementAndGet();
    }

    /**
     * Decrements the backup and increments the primary event counter by
     * a {@code delta}.
     * <p>
     * Please note that this method does not guarantee atomicity for
     * adjusting the two counters.
     *
     * @param delta The value the backup event counter to be decreased
     *              and the primary event counter to be increased by
     */
    void moveFromBackupToPrimaryCounter(int delta) {
        backupElementCounter.addAndGet(-delta);
        primaryElementCounter.addAndGet(delta);
    }

    /**
     * Decrements the primary and increments the backup event counter by
     * a {@code delta}.
     * <p>
     * Please note that this method does not guarantee atomicity for
     * adjusting the two counters.
     *
     * @param delta The value the backup event counter to be decreased
     *              and the primary event counter to be increased by
     */
    void moveFromPrimaryToBackupCounter(int delta) {
        primaryElementCounter.addAndGet(-delta);
        backupElementCounter.addAndGet(delta);
    }

    /**
     * Returns the count of the events in the WAN replication queues
     * belong to primary partitions.
     */
    int getPrimaryElementCount() {
        return primaryElementCounter.get();
    }

    /**
     * Returns the count of the events in the WAN replication queues
     * belong to backup partitions.
     */
    int getBackupElementCount() {
        return backupElementCounter.get();
    }

    /**
     * Sets the primary event counter to a new value
     *
     * @param newValue The value to be set
     */
    void setPrimaryElementCounter(int newValue) {
        primaryElementCounter.set(newValue);
    }

    /**
     * Sets the backup event counter to a new value
     *
     * @param newValue The value to be set
     */
    void setBackupElementCounter(int newValue) {
        backupElementCounter.set(newValue);
    }

    @Override
    public String toString() {
        return "WanElementCounter{"
                + "primaryElementCounter=" + primaryElementCounter
                + ", backupElementCounter=" + backupElementCounter
                + '}';
    }
}
