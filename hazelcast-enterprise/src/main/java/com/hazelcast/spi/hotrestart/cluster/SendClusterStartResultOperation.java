package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.internal.cluster.impl.operations.JoinOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.hotrestart.HotRestartIntegrationService;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterStartStatus.CLUSTER_START_FAILED;

/**
 *
 * Operation, which is used to send cluster-wide validation result (success or failure) among the members after the
 * cluster-wide validation phase completes. It can be send from master to members and members to master.
 *
 */
public class SendClusterStartResultOperation extends Operation implements JoinOperation {


    private HotRestartClusterStartStatus result;

    private Set<String> excludedMemberUuids;

    private ClusterState clusterState;

    public SendClusterStartResultOperation() {
    }

    public SendClusterStartResultOperation(HotRestartClusterStartStatus result,
                                           Set<String> excludedMemberUuids,
                                           ClusterState clusterState) {
        this.result = result;
        this.excludedMemberUuids = excludedMemberUuids == null ? Collections.emptySet() : excludedMemberUuids;
        this.clusterState = clusterState;
    }

    public static SendClusterStartResultOperation newFailureResultOperation() {
        return new SendClusterStartResultOperation(CLUSTER_START_FAILED, null, null);
    }

    @Override
    public void run() throws Exception {
        Address caller = getCallerAddress();
        HotRestartIntegrationService service = getService();
        ClusterMetadataManager clusterMetadataManager = service.getClusterMetadataManager();
        clusterMetadataManager.receiveHotRestartStatus(caller, result, excludedMemberUuids, clusterState);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public String getServiceName() {
        return HotRestartIntegrationService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(result.name());
        out.writeInt(excludedMemberUuids.size());
        for (String uuid : excludedMemberUuids) {
            out.writeUTF(uuid);
        }
        boolean clusterStateExists = clusterState != null;
        out.writeBoolean(clusterStateExists);
        if (clusterStateExists) {
            out.writeUTF(clusterState.name());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        result = HotRestartClusterStartStatus.valueOf(in.readUTF());
        int uuidCount = in.readInt();
        excludedMemberUuids = new HashSet<>();
        for (int i = 0; i < uuidCount; i++) {
            excludedMemberUuids.add(in.readUTF());
        }
        boolean clusterStateExists = in.readBoolean();
        if (clusterStateExists) {
            clusterState = ClusterState.valueOf(in.readUTF());
        }
    }

    @Override
    public int getFactoryId() {
        return HotRestartClusterSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return HotRestartClusterSerializerHook.SEND_CLUSTER_START_RESULT;
    }
}
