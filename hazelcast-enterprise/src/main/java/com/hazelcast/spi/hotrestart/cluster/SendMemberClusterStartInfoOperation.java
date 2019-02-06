package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.cluster.impl.operations.JoinOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.hotrestart.HotRestartIntegrationService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

/**
 * Operation, which is used to send local partition table to master member
 * during cluster-wide validation phase.
 */
public class SendMemberClusterStartInfoOperation extends Operation implements JoinOperation, Versioned {

    private MemberClusterStartInfo memberClusterStartInfo;

    public SendMemberClusterStartInfoOperation() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public SendMemberClusterStartInfoOperation(MemberClusterStartInfo memberClusterStartInfo) {
        this.memberClusterStartInfo = memberClusterStartInfo;
    }

    @Override
    public void run() throws Exception {
        HotRestartIntegrationService service = getService();
        ClusterMetadataManager clusterMetadataManager = service.getClusterMetadataManager();
        ClusterService clusterService = getNodeEngine().getClusterService();
        Member member = clusterService.getMember(getCallerAddress(), getCallerUuid());
        if (member == null) {
            getLogger().warning("An unknown member sent MemberClusterStartInfo. Address: "
                    + getCallerAddress() + ", UUID: " + getCallerUuid());
            return;
        }
        clusterMetadataManager.receiveClusterStartInfoFromMember(member, memberClusterStartInfo);
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
        if (out.getVersion().isGreaterOrEqual(Versions.V3_12)) {
            out.writeObject(memberClusterStartInfo);
        } else {
            memberClusterStartInfo.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        if (in.getVersion().isGreaterOrEqual(Versions.V3_12)) {
            memberClusterStartInfo = in.readObject();
        } else {
            memberClusterStartInfo = new MemberClusterStartInfo();
            memberClusterStartInfo.readData(in);
        }
    }

    @Override
    public int getFactoryId() {
        return HotRestartClusterSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return HotRestartClusterSerializerHook.SEND_MEMBER_CLUSTER_START_INFO;
    }

}
