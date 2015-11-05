package com.hazelcast.enterprise.wan.operation;

import com.hazelcast.cluster.impl.operations.WanReplicationOperation;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;

/**
 * Operation to be sent to target WAN members
 */
public class WanOperation extends AbstractOperation implements WanReplicationOperation {

    private Data event;
    private WanAcknowledgeType acknowledgeType;

    public WanOperation() { }

    public WanOperation(Data event, WanAcknowledgeType acknowledgeType) {
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
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeData(event);
        out.writeInt(acknowledgeType.getId());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        event = in.readData();
        acknowledgeType = WanAcknowledgeType.getById(in.readInt());
    }

    public WanAcknowledgeType getAcknowledgeType() {
        return acknowledgeType;
    }
}
