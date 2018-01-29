package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.enterprise.wan.BatchWanReplicationEvent;
import com.hazelcast.nio.Address;

/**
 * The WanBatchSender is responsible for transmitting the WAN event batch
 * from this node to a target address (on the target cluster).
 */
public interface WanBatchSender {

    /**
     * Sends the WAN batch to the target address
     *
     * @param batchReplicationEvent the WAN batch events
     * @param target                the target address
     * @return {@code true} if the batch was sent successfully
     */
    boolean send(BatchWanReplicationEvent batchReplicationEvent, Address target);
}
