package com.hazelcast.enterprise.wan.operation;

import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

/**
 * Base class for wan replication operations
 */
public abstract class EWRBaseOperation extends Operation
        implements PartitionAwareOperation, IdentifiedDataSerializable {

    protected transient Object response;

    String wanReplicationName;
    String targetName;
    String serviceName;

    protected EWRBaseOperation() { }

    protected EWRBaseOperation(String wanReplicationName, String targetName) {
        this.wanReplicationName = wanReplicationName;
        this.targetName = targetName;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public String getServiceName() {
        return EnterpriseWanReplicationService.SERVICE_NAME;
    }

    protected EnterpriseWanReplicationService getEWRService() {
        return (EnterpriseWanReplicationService) getNodeEngine().getWanReplicationService();
    }

    @Override
    public void beforeRun() throws Exception { }

    @Override
    public void afterRun() throws Exception { }

    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(wanReplicationName);
        out.writeUTF(targetName);
        out.writeUTF(serviceName);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        wanReplicationName = in.readUTF();
        targetName = in.readUTF();
        serviceName = in.readUTF();
    }
}
