package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.internal.cluster.impl.operations.JoinOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.hotrestart.HotRestartIntegrationService;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Sends the list of members, that is sufficient to pass await join step,
 * to the cluster members, when partial data recovery is triggered
 */
public class SendExpectedMemberUuidsOperation extends Operation implements JoinOperation {

    private Set<String> expectedMemberUuids;

    public SendExpectedMemberUuidsOperation() {
    }

    public SendExpectedMemberUuidsOperation(Set<String> expectedMemberUuids) {
        this.expectedMemberUuids = expectedMemberUuids;
    }

    @Override
    public void run() throws Exception {
        HotRestartIntegrationService service = getService();
        ClusterMetadataManager clusterMetadataManager = service.getClusterMetadataManager();
        clusterMetadataManager.receiveExpectedMembersFromMaster(getCallerAddress(), expectedMemberUuids);
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
        out.writeInt(expectedMemberUuids.size());
        for (String uuid : expectedMemberUuids) {
            out.writeUTF(uuid);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        expectedMemberUuids = new HashSet<String>();
        for (int i = 0; i < size; i++) {
            expectedMemberUuids.add(in.readUTF());
        }
    }

    @Override
    public int getFactoryId() {
        return HotRestartClusterSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return HotRestartClusterSerializerHook.SEND_EXPECTED_MEMBERS;
    }

}
