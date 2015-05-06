package com.hazelcast.map.impl.querycache.event.sequence;

/**
 * Provides sequences for subscriber side.
 *
 * @see PartitionSequencer
 */
public interface SubscriberSequencerProvider {

    /**
     * Atomically sets the value of sequence number for the partition.
     *
     * @param expect the expected sequence
     * @param update the new sequence
     * @return {@code true} if cas operation is successful, otherwise returns {@code false}
     */
    boolean compareAndSetSequence(long expect, long update, int partitionId);

    /**
     * Returns partitions current sequence number.
     *
     * @return current sequence number.
     */
    long getSequence(int partitionId);
}
