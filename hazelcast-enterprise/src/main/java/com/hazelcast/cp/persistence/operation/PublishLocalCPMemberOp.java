package com.hazelcast.cp.persistence.operation;

import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftSystemOperation;
import com.hazelcast.cp.persistence.CPPersistenceDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.RaftInvocationContext;

import java.io.IOException;

/**
 * If a Hazelcast member restores the state from persistent storage
 * and it detects that local IP address is different than before shutdown,
 * then it broadcasts its new CP member information to other cluster members using this
 * operation until it commits the change to the metadata CP group.
 * <p/>
 * Please note that this operation is not a {@link com.hazelcast.cp.internal.RaftOp},
 * so it is not handled via the Raft layer.
 */
public class PublishLocalCPMemberOp extends Operation implements IdentifiedDataSerializable, RaftSystemOperation {

    private CPMemberInfo localMember;

    public PublishLocalCPMemberOp() {
    }

    public PublishLocalCPMemberOp(CPMemberInfo localMember) {
        this.localMember = localMember;
    }

    @Override
    public void run() {
        RaftService service = getService();
        RaftInvocationContext raftInvocationContext = service.getInvocationManager().getRaftInvocationContext();
        raftInvocationContext.updateMember(localMember);
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
        return CPPersistenceDataSerializerHook.PUBLISH_LOCAL_MEMBER_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(localMember);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        localMember = in.readObject();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", member=").append(localMember);
    }
}
