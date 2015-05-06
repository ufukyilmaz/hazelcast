package com.hazelcast.map.impl.querycache;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;

import java.util.concurrent.Future;

/**
 * Provides abstraction over client and node side invocations.
 */
public interface InvokerWrapper {

    Future invokeOnPartitionOwner(Object request, int partitionId);

    Object invokeOnAllPartitions(Object request) throws Exception;

    Future invokeOnTarget(Object operation, Address address);

    Object invoke(Object operation);

    void executeOperation(Operation op);
}
