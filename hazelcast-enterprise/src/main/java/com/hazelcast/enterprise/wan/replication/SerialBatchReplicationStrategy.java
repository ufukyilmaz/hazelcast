package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.nio.Address;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WAN replication strategy which will send events for a single partition ID
 * only to a single, dedicated target endpoint if the endpoint list is not
 * changing.
 * This will, if there are no changes in the target endpoint list, ensure
 * ordering of partition events.
 */
public class SerialBatchReplicationStrategy implements BatchReplicationStrategy {

    private final Set<Address> currentEndpointInvocations
            = Collections.newSetFromMap(new ConcurrentHashMap<Address, Boolean>());
    private int endpointIndexOffset;

    @Override
    public Address getNextEventBatchEndpoint(List<Address> endpoints) {
        if (endpoints.isEmpty()) {
            return null;
        }
        int endpointCount = endpoints.size();
        endpointIndexOffset = (endpointIndexOffset + 1) % endpointCount;

        for (int i = 0; i < endpointCount; i++) {
            Address endpoint = endpoints.get((i + endpointIndexOffset) % endpointCount);
            if (currentEndpointInvocations.add(endpoint)) {
                return endpoint;
            }
        }
        return null;
    }

    @Override
    public void complete(Address endpoint) {
        currentEndpointInvocations.remove(endpoint);
    }

    @Override
    public int getFirstPartitionId(Address endpoint, List<Address> endpoints) {
        return endpoints.indexOf(endpoint);
    }

    @Override
    public int getPartitionIdStep(Address endpoint, List<Address> endpoints) {
        return endpoints.size();
    }


    @Override
    public String toString() {
        return "SerialBatchReplicationStrategy{currentEndpointInvocations=" + currentEndpointInvocations + '}';
    }
}
