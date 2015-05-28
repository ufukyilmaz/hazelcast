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
     * @param expect      the expected sequence
     * @param update      the new sequence
     * @param partitionId id of the partition
     * @return {@code true} if cas operation is successful, otherwise returns {@code false}
     */
    boolean compareAndSetSequence(long expect, long update, int partitionId);

    /**
     * Returns partitions current sequence number.
     *
     * @param partitionId id of the partition
     * @return current sequence number.
     */
    long getSequence(int partitionId);


    /**
     * Resets the sequence number for the supplied {@code partition} to zero.
     *
     * @param partitionId id of the partition
     */
    void reset(int partitionId);
}
