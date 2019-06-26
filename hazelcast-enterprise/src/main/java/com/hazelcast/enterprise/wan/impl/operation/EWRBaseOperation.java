package com.hazelcast.enterprise.wan.impl.operation;

import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import java.io.IOException;

/**
 * Base class for WAN replication operations.
 */
public abstract class EWRBaseOperation extends Operation implements PartitionAwareOperation, IdentifiedDataSerializable {

    protected transient Object response;

    String wanReplicationName;
    String wanPublisherId;
    // service name is always null but it can't be easily removed
    // because of backwards compatibility (rolling upgrade)
    String serviceName;

    protected EWRBaseOperation() {
    }

    protected EWRBaseOperation(String wanReplicationName, String wanPublisherId) {
        this.wanReplicationName = wanReplicationName;
        this.wanPublisherId = wanPublisherId;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    EnterpriseWanReplicationService getEWRService() {
        return (EnterpriseWanReplicationService) getNodeEngine().getWanReplicationService();
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(wanReplicationName);
        out.writeUTF(wanPublisherId);
        out.writeUTF(serviceName);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        wanReplicationName = in.readUTF();
        wanPublisherId = in.readUTF();
        serviceName = in.readUTF();
    }
}
