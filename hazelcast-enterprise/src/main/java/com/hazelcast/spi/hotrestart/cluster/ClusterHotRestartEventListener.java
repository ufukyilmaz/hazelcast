package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.annotation.PrivateApi;

import java.util.Collection;
import java.util.EventListener;

/**
 * This is a sync listener. When a node calls one of the methods below during hot restart, it will block until the method returns.
 */
@PrivateApi
public abstract class ClusterHotRestartEventListener
        implements EventListener {



    /**
     * Called after a node completes the preparation by reading member list and partition table from disk
     *
     * @param members             member list which is read from the disk
     * @param partitionTable      partition table which is read from disk
     * @param startWithHotRestart if false, node will start without hot restart
     */
    public void onPrepareComplete(Collection<Address> members, Address[][] partitionTable, boolean startWithHotRestart) {
    }

    /**
     * Called when a persisted member list contains only a single member
     */
    public void onSingleMemberCluster() {

    }

    /**
     * Called on all nodes after all expected members joins to the cluster
     *
     * @param members expected member list
     */
    public void onAllMembersJoin(Collection<Address> members) {

    }

    /**
     * Called after master receives a partition table from a member and validates it
     *
     * @param sender  member that has sent the partition table
     * @param success result of the validation
     */
    public void onPartitionTableValidationResult(Address sender, boolean success) {

    }

    /**
     * Called on master when it receives partition tables from all nodes and called on non-master nodes when they receive the
     * partition table validation status from master
     *
     * @param result result of the partition table validation
     */
    public void onPartitionTableValidationComplete(HotRestartClusterInitializationStatus result) {

    }

    /**
     * Called on master when it receives a hot-restart data load result from a node
     *
     * @param sender  address of the node that sends its hot-restart data load result
     * @param success result of the hot restart data load attempt of the node
     */
    public void onHotRestartDataLoadResult(Address sender, boolean success) {

    }

    /**
     * Called on master when it receives a failure result from one of the nodes, and called on non-master nodes when they receive
     * the result of the load process from master
     *
     * @param result result of the cluster wide load operation
     */
    public void onHotRestartDataLoadComplete(HotRestartClusterInitializationStatus result) {

    }

}
