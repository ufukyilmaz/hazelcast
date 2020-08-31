package com.hazelcast.internal.partition.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.PartitionReplica;

import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getPartitionService;

/**
 * Utility class to access package-private partition methods.
 */
public class PartitionTableUtil {

    public static PartitionReplica[] getReplicas(HazelcastInstance instance, int partitionId) {
        PartitionStateManager partitionStateManager = getPartitionStateManager(instance);
        return partitionStateManager.getPartitionImpl(partitionId).getReplicasCopy();
    }

    public static void updateReplicas(HazelcastInstance instance, int partitionId, PartitionReplica[] replicas) {
        PartitionStateManager partitionStateManager = getPartitionStateManager(instance);
        partitionStateManager.getPartitionImpl(partitionId).setReplicas(replicas);
        getNode(instance).getNodeExtension().onPartitionStateChange();
    }

    private static PartitionStateManager getPartitionStateManager(HazelcastInstance instance) {
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instance);
        return partitionService.getPartitionStateManager();
    }
}
