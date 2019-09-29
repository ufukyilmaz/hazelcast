package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.cluster.Member;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.spi.annotation.PrivateApi;

import java.util.Collection;
import java.util.EventListener;
import java.util.Set;
import java.util.UUID;

/**
 * This is a sync listener. When a node calls one of the methods below during Hot Restart, it will block until the method returns.
 */
@PrivateApi
public abstract class ClusterHotRestartEventListener implements EventListener {


    /**
     * Called after a node completes the preparation by reading member list and partition table from disk
     *  @param members             member list which is read from the disk
     * @param partitionTable      partition table which is read from disk
     * @param startWithHotRestart if false, node will start without hot restart
     */
    public void onPrepareComplete(Collection<? extends Member> members, PartitionTableView partitionTable,
            boolean startWithHotRestart) {
    }

    /**
     * Called when a persisted member list contains only a single member.
     */
    public void onSingleMemberCluster() {

    }

    /**
     * Called while waiting for all members to join.
     * @param currentMembers currently joined members
     */
    public void beforeAllMembersJoin(Collection<? extends Member> currentMembers) {

    }

    /**
     * Called on all nodes after all expected members join to the cluster.
     *
     * @param members expected member list
     */
    public void afterExpectedMembersJoin(Collection<? extends Member> members) {

    }

    /**
     * Called after master receives a partition table from a member and validates it.
     *  @param sender  member that has sent the partition table
     * @param success result of the validation
     */
    public void onPartitionTableValidationResult(Member sender, boolean success) {

    }

    /**
     * Called when data load is started.
     * @param member member that starts to load its own data
     */
    public void onDataLoadStart(Member member) {

    }

    /**
     * Called on master when it receives a Hot Restart data load result from a node.
     *  @param sender member that sends its hot-restart data load result
     * @param success result of the hot restart data load attempt of the node
     */
    public void onHotRestartDataLoadResult(Member sender, boolean success) {

    }

    /**
     * Called on all nodes after the final cluster start decision is made.
     *
     * @param result result of the cluster wide load operation
     */
    public void onHotRestartDataLoadComplete(HotRestartClusterStartStatus result, Set<UUID> excludedMemberUuids) {

    }
}
