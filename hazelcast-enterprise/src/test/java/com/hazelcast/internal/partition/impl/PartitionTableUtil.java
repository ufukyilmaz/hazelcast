package com.hazelcast.internal.partition.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.PartitionReplica;

import java.util.Arrays;

import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.getPartitionService;

/**
 * Utility class to access package-private partition methods.
 */
public class PartitionTableUtil {

    public static PartitionReplica[] getReplicas(HazelcastInstance instance, int partitionId) {
        PartitionStateManager partitionStateManager = getPartitionStateManager(instance);
        return Arrays.copyOf(partitionStateManager.getPartitionImpl(partitionId).getReplicas(), MAX_REPLICA_COUNT);
    }

    public static void updateReplicas(HazelcastInstance instance, int partitionId, PartitionReplica[] replicas) {
        PartitionStateManager partitionStateManager = getPartitionStateManager(instance);
        partitionStateManager.updateReplicas(partitionId, replicas);
        getNode(instance).getNodeExtension().onPartitionStateChange();
    }

    private static PartitionStateManager getPartitionStateManager(HazelcastInstance instance) {
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instance);
        return partitionService.getPartitionStateManager();
    }
}
