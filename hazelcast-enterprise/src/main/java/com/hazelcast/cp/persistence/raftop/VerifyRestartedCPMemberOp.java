package com.hazelcast.cp.persistence.raftop;

import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.MetadataRaftGroupManager;
import com.hazelcast.cp.internal.raftop.metadata.MetadataRaftGroupOp;
import com.hazelcast.cp.persistence.CPPersistenceDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * If a Hazelcast member restores the state from persistent storage,
 * then commits this operation to the metadata CP group to verify that
 * it is still a member of CP Subsystem.
 * <p>
 * If member's local IP address has changed, then its identity will be
 * updated in the active members list.
 */
public class VerifyRestartedCPMemberOp extends MetadataRaftGroupOp implements IdentifiedDataSerializable,
                                                                              IndeterminateOperationStateAware {

    private CPMemberInfo member;

    public VerifyRestartedCPMemberOp() {
    }

    public VerifyRestartedCPMemberOp(CPMemberInfo member) {
        this.member = member;
    }

    @Override
    public Object run(MetadataRaftGroupManager metadataGroupManager, long commitIndex) throws Exception {
        metadataGroupManager.verifyRestartedMember(commitIndex, member);
        return null;
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    public CPMemberInfo getMember() {
        return member;
    }

    @Override
    public int getFactoryId() {
        return CPPersistenceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CPPersistenceDataSerializerHook.VERIFY_RESTARTED_MEMBER_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(member);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        member = in.readObject();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", member=").append(member);
    }
}
