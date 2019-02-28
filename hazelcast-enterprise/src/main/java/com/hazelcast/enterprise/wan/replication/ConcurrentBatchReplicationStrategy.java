package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.nio.Address;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;

/**
 * WAN replication strategy which will send up to a configurable number of
 * batches concurrently and randomly to the available target endpoints. Since
 * batches of events for the same partition ID are sent concurrently, events
 * for the same partition can be reordered. This makes this replication
 * strategy adequate only for events which are commutative.
 */
public class ConcurrentBatchReplicationStrategy implements BatchReplicationStrategy {

    private final Semaphore concurrentInvocations;
    private final int maxConcurrentInvocations;
    private final Random random;

    public ConcurrentBatchReplicationStrategy(int maxConcurrentInvocations) {
        this.concurrentInvocations = new Semaphore(maxConcurrentInvocations);
        this.maxConcurrentInvocations = maxConcurrentInvocations;
        this.random = new Random();
    }

    @Override
    public Address getNextEventBatchEndpoint(List<Address> endpoints) {
        if (!concurrentInvocations.tryAcquire()) {
            return null;
        }

        if (endpoints.isEmpty()) {
            // no available endpoints
            concurrentInvocations.release();
            return null;
        }

        return endpoints.get(random.nextInt(endpoints.size()));
    }

    @Override
    public void complete(Address endpoint) {
        concurrentInvocations.release();
    }

    @Override
    public int getFirstPartitionId(Address endpoint, List<Address> endpoints) {
        return 0;
    }

    @Override
    public int getPartitionIdStep(Address endpoint, List<Address> endpoints) {
        return 1;
    }

    @Override
    public boolean hasOngoingReplication() {
        return concurrentInvocations.availablePermits() < maxConcurrentInvocations;
    }

    @Override
    public String toString() {
        return "ConcurrentBatchReplicationStrategy{availablePermits=" + concurrentInvocations.availablePermits() + '}';
    }
}
