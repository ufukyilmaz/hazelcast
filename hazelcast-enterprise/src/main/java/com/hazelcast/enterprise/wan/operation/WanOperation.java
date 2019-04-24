package com.hazelcast.enterprise.wan.operation;

import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.internal.cluster.impl.operations.WanReplicationOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * Operation sent from the source WAN endpoint to the target endpoint.
 * This operation contains the changes in the source endpoint (source
 * cluster) and the acknowledge type. The acknowledge type defines when
 * the response for this operation will be sent to the source endpoint so
 * that it knows in which stage is the operation (accepted or completed).
 */
public class WanOperation extends Operation implements WanReplicationOperation, IdentifiedDataSerializable {

    private IdentifiedDataSerializable event;
    private WanAcknowledgeType acknowledgeType;

    public WanOperation() {
    }

    public WanOperation(IdentifiedDataSerializable event, WanAcknowledgeType acknowledgeType) {
        this.event = event;
        this.acknowledgeType = acknowledgeType;
    }

    @Override
    public void run() throws Exception {
        EnterpriseWanReplicationService wanRepService
                = (EnterpriseWanReplicationService) getNodeEngine().getWanReplicationService();
        wanRepService.handleEvent(event, this);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EWRDataSerializerHook.WAN_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(event);
        out.writeInt(acknowledgeType.getId());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        event = in.readObject();
        acknowledgeType = WanAcknowledgeType.getById(in.readInt());
    }

    public WanAcknowledgeType getAcknowledgeType() {
        return acknowledgeType;
    }
}
