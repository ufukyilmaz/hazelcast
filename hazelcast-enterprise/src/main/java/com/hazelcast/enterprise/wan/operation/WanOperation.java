package com.hazelcast.enterprise.wan.operation;

import com.hazelcast.cluster.impl.operations.WanReplicationOperation;
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

    Data event;
    private Boolean response = Boolean.TRUE;

    public WanOperation() { }

    public WanOperation(Data event) {
        this.event = event;
    }

    @Override
    public void run() throws Exception {
        EnterpriseWanReplicationService wanRepService
                = (EnterpriseWanReplicationService) getNodeEngine().getWanReplicationService();
        wanRepService.handleEvent(event);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeData(event);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        event = in.readData();
    }
}
