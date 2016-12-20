package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.internal.cluster.impl.operations.JoinOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.hotrestart.HotRestartIntegrationService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

/**
 * Operation, which is used to send local partition table to master member
 * during cluster-wide validation phase.
 */
public class SendMemberClusterStartInfoOperation extends Operation implements JoinOperation {

    private MemberClusterStartInfo memberClusterStartInfo;

    public SendMemberClusterStartInfoOperation() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public SendMemberClusterStartInfoOperation(MemberClusterStartInfo memberClusterStartInfo) {
        this.memberClusterStartInfo = memberClusterStartInfo;
    }

    @Override
    public void run()
            throws Exception {
        HotRestartIntegrationService service = getService();
        ClusterMetadataManager clusterMetadataManager = service.getClusterMetadataManager();
        clusterMetadataManager.receiveClusterStartInfoFromMember(getCallerAddress(), getCallerUuid(), memberClusterStartInfo);
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
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        memberClusterStartInfo.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        memberClusterStartInfo = new MemberClusterStartInfo();
        memberClusterStartInfo.readData(in);
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
