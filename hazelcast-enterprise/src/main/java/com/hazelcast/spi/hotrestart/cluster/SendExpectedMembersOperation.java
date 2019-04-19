package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.internal.cluster.impl.operations.JoinOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.hotrestart.HotRestartIntegrationService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Sends the list of members, that is sufficient to pass await join step,
 * to the cluster members, when partial data recovery is triggered
 */
public class SendExpectedMembersOperation extends Operation implements JoinOperation {

    private Map<String, Address> expectedMembers;

    public SendExpectedMembersOperation() {
    }

    public SendExpectedMembersOperation(Map<String, Address> expectedMembers) {
        this.expectedMembers = expectedMembers;
    }

    @Override
    public void run() throws Exception {
        HotRestartIntegrationService service = getService();
        ClusterMetadataManager clusterMetadataManager = service.getClusterMetadataManager();
        clusterMetadataManager.receiveExpectedMembersFromMaster(getCallerAddress(), expectedMembers);
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
        out.writeInt(expectedMembers.size());
        for (Map.Entry<String, Address> entry : expectedMembers.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        expectedMembers = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String uuid = in.readUTF();
            Address address = in.readObject();
            expectedMembers.put(uuid, address);
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
