package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.enterprise.wan.BatchWanReplicationEvent;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.nio.Address;

/**
 * The WanBatchSender is responsible for transmitting the WAN event batch
 * from this node to a target address (on the target cluster).
 */
public interface WanBatchSender {

    /**
     * Initialises this sender.
     *
     * @param node      the node on which this sender is running
     * @param publisher the WAN publisher for which this sender is used to send batches for
     */
    void init(Node node, WanBatchReplication publisher);

    /**
     * Sends the WAN batch to the target address and returns a future
     * representing the pending completion of the invocation.
     *
     * @param batchReplicationEvent the WAN batch events
     * @param target                the target address
     * @return {@code true} if the batch was sent successfully
     */
    ICompletableFuture<Boolean> send(BatchWanReplicationEvent batchReplicationEvent, Address target);
}
