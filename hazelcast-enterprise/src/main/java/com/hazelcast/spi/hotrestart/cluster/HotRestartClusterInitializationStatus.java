package com.hazelcast.spi.hotrestart.cluster;

/**
 * Indicates the state of the cluster from the perspective of a node during a Hot Restart cluster verification.
 */
public enum HotRestartClusterInitializationStatus {

    /**
     * Initial state that is set at the beginning of the cluster verification process.
     */
    PENDING_VERIFICATION,

    /**
     * Indicates that the cluster verification failed because of either mismatching partition tables or
     * not-loaded data on some nodes.
     */
    VERIFICATION_FAILED,

    /**
     * The state of a node after it discovers that its partition table matches with all other nodes.
     * After this state, it can move to {@link HotRestartClusterInitializationStatus#VERIFICATION_AND_LOAD_SUCCEEDED}
     * or {@link HotRestartClusterInitializationStatus#VERIFICATION_FAILED}
     */
    PARTITION_TABLE_VERIFIED,

    /**
     * Final state of the cluster after load process succeeds on all nodes.
     */
    VERIFICATION_AND_LOAD_SUCCEEDED,

    /**
     * Final state of the cluster after Hot Restart start process is interrupted and cluster is started with force start.
     */
    FORCE_STARTED

}
