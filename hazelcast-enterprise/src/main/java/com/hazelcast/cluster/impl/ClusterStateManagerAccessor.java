package com.hazelcast.cluster.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.nio.Address;

import java.util.Collection;

/**
 * Accessor used to invoke package-private
 * methods on ClusterStateManager in a safe manner.
 * <p/>
 * Normally these method are not allowed to be called,
 * they are not required for ClusterState logic.
 * <p/>
 * Hot-restart needs to set ClusterState directly
 * without a cluster-wide transaction, it's already managing a consensus of its own.
 */
public final class ClusterStateManagerAccessor {

    private ClusterStateManagerAccessor() {
    }

    public static void setClusterState(ClusterServiceImpl clusterService, ClusterState newState) {
        clusterService.getClusterStateManager().setClusterState(newState);
    }

    public static void addMembersRemovedInNotActiveState(ClusterServiceImpl clusterService,
            Collection<Address> addresses) {
        clusterService.addMembersRemovedInNotActiveState(addresses);
    }
}
