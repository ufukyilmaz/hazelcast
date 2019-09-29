package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.ClusterHotRestartStatus;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.MemberHotRestartStatus;
import com.hazelcast.nio.Address;
import com.hazelcast.internal.hotrestart.cluster.MemberClusterStartInfo.DataLoadStatus;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.hotrestart.cluster.HotRestartClusterStartStatus.CLUSTER_START_SUCCEEDED;

/**
 * Utility class to convert Hot Restart status data to Management Center DTO
 */
public final class ClusterHotRestartStatusDTOUtil {

    private ClusterHotRestartStatusDTOUtil() {
    }

    public static ClusterHotRestartStatusDTO create(ClusterMetadataManager clusterMetadataManager) {
        HotRestartClusterDataRecoveryPolicy dataRecoveryPolicy = clusterMetadataManager.getClusterDataRecoveryPolicy();

        ClusterHotRestartStatus hotRestartStatus = getClusterHotRestartStatus(clusterMetadataManager);

        long remainingValidationTimeMillis = clusterMetadataManager.getRemainingValidationTimeMillis();
        long remainingDataLoadTimeMillis = clusterMetadataManager.getRemainingDataLoadTimeMillis();

        Map<String, MemberHotRestartStatus> memberHotRestartStatusMap =
                getMemberHotRestartStatusMap(clusterMetadataManager);

        return new ClusterHotRestartStatusDTO(dataRecoveryPolicy, hotRestartStatus, remainingValidationTimeMillis,
                remainingDataLoadTimeMillis, memberHotRestartStatusMap);
    }

    private static Map<String, MemberHotRestartStatus> getMemberHotRestartStatusMap(
            ClusterMetadataManager clusterMetadataManager) {

        Collection<MemberImpl> restoredMembers = clusterMetadataManager.getRestoredMembers();
        Map<String, MemberHotRestartStatus> memberHotRestartStatusMap = new HashMap<>(restoredMembers.size());

        for (MemberImpl member : restoredMembers) {
            Address address = member.getAddress();
            memberHotRestartStatusMap.put(address.getHost() + ":" + address.getPort(),
                    getMemberHotRestartStatus(clusterMetadataManager, member));
        }
        return memberHotRestartStatusMap;
    }

    private static MemberHotRestartStatus getMemberHotRestartStatus(ClusterMetadataManager clusterMetadataManager,
            MemberImpl member) {

        DataLoadStatus dataLoadStatus = clusterMetadataManager.getMemberDataLoadStatus(member);
        if (dataLoadStatus == null) {
            // either data load not started yet or it's completed successfully and per-member stats are removed
            return clusterMetadataManager.getHotRestartStatus()
                    == CLUSTER_START_SUCCEEDED ? MemberHotRestartStatus.SUCCESSFUL : MemberHotRestartStatus.PENDING;
        }

        switch (dataLoadStatus) {
            case LOAD_IN_PROGRESS:
                return MemberHotRestartStatus.LOAD_IN_PROGRESS;
            case LOAD_SUCCESSFUL:
                return MemberHotRestartStatus.SUCCESSFUL;
            case LOAD_FAILED:
                return MemberHotRestartStatus.FAILED;
            default:
                throw new IllegalArgumentException("Unknown status: " + dataLoadStatus);
        }
    }

    private static ClusterHotRestartStatus getClusterHotRestartStatus(ClusterMetadataManager clusterMetadataManager) {
        HotRestartClusterStartStatus clusterStartStatus = clusterMetadataManager.getHotRestartStatus();
        switch (clusterStartStatus) {
            case CLUSTER_START_IN_PROGRESS:
                return ClusterHotRestartStatus.IN_PROGRESS;
            case CLUSTER_START_SUCCEEDED:
                return ClusterHotRestartStatus.SUCCEEDED;
            case CLUSTER_START_FAILED:
                return ClusterHotRestartStatus.FAILED;
            default:
                throw new IllegalArgumentException("Unknown status: " + clusterStartStatus);
        }
    }
}
