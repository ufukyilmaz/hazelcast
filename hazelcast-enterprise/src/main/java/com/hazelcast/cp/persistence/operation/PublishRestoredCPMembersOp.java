package com.hazelcast.cp.persistence.operation;

import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftSystemOperation;
import com.hazelcast.cp.persistence.CPPersistenceDataSerializerHook;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * If a Hazelcast member restores the initial CP member list from persistent
 * storage, it broadcasts this list to other cluster members using this
 * operation until the Metadata CP group elects its leader.
 * <p/>
 * Please note that this operation is not a {@link com.hazelcast.cp.internal.RaftOp},
 * so it is not handled via the Raft layer.
 */
public class PublishRestoredCPMembersOp extends Operation implements IdentifiedDataSerializable, RaftSystemOperation {

    private RaftGroupId metadataGroupId;
    private long membersCommitIndex;
    private Collection<CPMemberInfo> members;

    public PublishRestoredCPMembersOp() {
    }

    public PublishRestoredCPMembersOp(RaftGroupId metadataGroupId, long membersCommitIndex, Collection<CPMemberInfo> members) {
        this.metadataGroupId = metadataGroupId;
        this.membersCommitIndex = membersCommitIndex;
        this.members = members;
    }

    @Override
    public void run() {
        RaftService service = getService();
        if (service.updateInvocationManagerMembers(metadataGroupId.getSeed(), membersCommitIndex, members)) {
            ILogger logger = getNodeEngine().getLogger(getClass());
            if (logger.isFineEnabled()) {
                logger.fine("Received restored seed: " + metadataGroupId.getSeed()
                        + ", members commit index: " + membersCommitIndex + ", CP member list: " + members);
            }
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return CPPersistenceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CPPersistenceDataSerializerHook.PUBLISH_RESTORED_MEMBERS_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(metadataGroupId);
        out.writeLong(membersCommitIndex);
        out.writeInt(members.size());
        for (CPMemberInfo member : members) {
            out.writeObject(member);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        metadataGroupId = in.readObject();
        membersCommitIndex = in.readLong();
        int len = in.readInt();
        members = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            CPMemberInfo member = in.readObject();
            members.add(member);
        }
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", metadataGroupId=").append(metadataGroupId)
          .append(", membersCommitIndex").append(membersCommitIndex)
          .append(", members=").append(members);
    }

}
