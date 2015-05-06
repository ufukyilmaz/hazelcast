package com.hazelcast.map.impl.querycache.event.sequence;

/**
 * This interface should be implemented by events which need a sequence number.
 */
public interface Sequenced {

    /**
     * Returns the sequence number.
     *
     * @return sequence number.
     */
    long getSequence();

    /**
     * Returns partition id which this sequence belongs to.
     *
     * @return partition id which this sequence belongs to.
     */
    int getPartitionId();

    /**
     * Sets sequence.
     *
     * @param sequence the sequence number to be set.
     */
    void setSequence(long sequence);
}
