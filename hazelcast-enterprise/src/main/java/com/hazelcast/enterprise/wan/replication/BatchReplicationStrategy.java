package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.nio.Address;

import java.util.List;

/**
 * Strategy defining the high-level WAN replication mechanism. Different
 * replication strategies may choose different ways in which batches are
 * sent to target endpoints.
 */
public interface BatchReplicationStrategy {
    /**
     * Returns the next available target endpoint from a provided list of
     * available target endpoints.
     * This method may acquire (reserve) the returned endpoint. It is the
     * responsibility of the user of this strategy to call the
     * {@link #complete(Address)} method with the returned address to mark
     * the endpoint as available again.
     *
     * @param endpoints the available target endpoints
     * @return the next available endpoint to which an event batch can be sent
     */
    Address getNextEventBatchEndpoint(List<Address> endpoints);

    /**
     * Releases the provided target endpoint as available for sending the next
     * event batch.
     *
     * @param endpoint the endpoint to release
     */
    void complete(Address endpoint);

    /**
     * Returns the first partition ID of the events which should be sent to the
     * provided target endpoint.
     *
     * @param endpoint  the target endpoint
     * @param endpoints the list of available target endpoints
     * @return the first partition ID of the events for which the target endpoint is
     * responsible for
     */
    int getFirstPartitionId(Address endpoint, List<Address> endpoints);

    /**
     * Returns the step between partition IDs of events which should be sent
     * to the provided target endpoint.
     *
     * @param endpoint  the target endpoint
     * @param endpoints the list of available target endpoints
     * @return the step between partition IDs of the events which should be sent to the
     * target endpoint
     */
    int getPartitionIdStep(Address endpoint, List<Address> endpoints);

    /**
     * Returns true if there is an ongoing replication process.
     */
    boolean hasOngoingReplication();
}
