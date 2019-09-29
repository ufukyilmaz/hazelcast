package com.hazelcast.internal.hotrestart.cluster;

/**
 * Indicates the state of the cluster from the perspective of a node during a hot restart cluster verification
 */
public enum HotRestartClusterStartStatus {

    /**
     * Initial state that is set at the beginning of the cluster verification process.
     */
    CLUSTER_START_IN_PROGRESS,

    /**
     * Indicates that the cluster verification failed because of either mismatching partition tables or
     * not-loaded data on some nodes
     */
    CLUSTER_START_FAILED,

    /**
     * Final state of the cluster after load process succeeds on all nodes
     */
    CLUSTER_START_SUCCEEDED,

}
