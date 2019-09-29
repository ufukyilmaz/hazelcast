package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.internal.cluster.impl.operations.JoinOperation;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.hotrestart.HotRestartIntegrationService;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Sends the list of members, that is sufficient to pass await join step,
 * to the cluster members, when partial data recovery is triggered
 */
public class SendExpectedMembersOperation extends Operation implements JoinOperation {

    private Map<UUID, Address> expectedMembers;

    public SendExpectedMembersOperation() {
    }

    public SendExpectedMembersOperation(Map<UUID, Address> expectedMembers) {
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
        for (Map.Entry<UUID, Address> entry : expectedMembers.entrySet()) {
            UUIDSerializationUtil.writeUUID(out, entry.getKey());
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
            UUID uuid = UUIDSerializationUtil.readUUID(in);
            Address address = in.readObject();
            expectedMembers.put(uuid, address);
        }
    }

    @Override
    public int getFactoryId() {
        return HotRestartClusterSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return HotRestartClusterSerializerHook.SEND_EXPECTED_MEMBERS;
    }
}
