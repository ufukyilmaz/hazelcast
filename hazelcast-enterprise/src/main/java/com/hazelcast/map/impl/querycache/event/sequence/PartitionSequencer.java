package com.hazelcast.map.impl.querycache.event.sequence;

/**
 * General contract of a sequence-number provider for a partition.
 * <p/>
 * Each partition has its own sequence-number on publisher-side. For every generated event, this sequence will be
 * incremented by one and set to that event before sending it to the subscriber side.
 * <p/>
 * On subscriber side, this sequence is used to keep track of published events; upon arrival of an event, there
 * is a pre-condition check to decide whether that just arrived event has next expected sequence-number for the
 * relevant partition. If the event has next-sequence, it is applied to the query-cache.
 * <p/>
 * Implementations of this interface should be thread-safe.
 */
public interface PartitionSequencer {

    /**
     * Returns next number in sequence for the partition.
     *
     * @return next number in sequence.
     */
    long nextSequence();

    /**
     * Sets the current sequence number for the partition.
     *
     * @param update new sequence number
     */
    void setSequence(long update);

    /**
     * Atomically sets the value of sequence number for the partition.
     *
     * @param expect the expected sequence
     * @param update the new sequence
     * @return {@code true} if cas operation is successful, otherwise returns {@code false}
     */
    boolean compareAndSetSequence(long expect, long update);

    /**
     * Returns current sequence number of the partition.
     *
     * @return current sequence number.
     */
    long getSequence();

    /**
     * Resets {@link PartitionSequencer}.
     * After this call returns sequence generation will be start from zero.
     */
    void reset();
}
