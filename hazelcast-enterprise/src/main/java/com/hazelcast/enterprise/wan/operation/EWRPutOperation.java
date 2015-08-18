package com.hazelcast.enterprise.wan.operation;

import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WanReplicationEvent;

import java.io.IOException;

/**
 * Used to put forwarded wan events to event queue
 */
public class EWRPutOperation extends EWRBackupAwareOperation
        implements IdentifiedDataSerializable {

    Data event;

    public EWRPutOperation() { }

    public EWRPutOperation(String wanReplicationName,
                           String targetName,
                           Data event, int backupCount) {
        super(wanReplicationName, targetName, backupCount);
        this.event = event;

    }

    @Override
    public void run() throws Exception {
        EnterpriseWanReplicationService wanReplicationService = getEWRService();
        assert wanReplicationService.getWanReplicationPublisher(wanReplicationName) != null;
        WanReplicationEndpoint endpoint = wanReplicationService.getEndpoint(wanReplicationName, targetName);
        WanReplicationEvent wanReplicationEvent = getNodeEngine().toObject(event);
        endpoint.publishReplicationEvent(wanReplicationEvent.getServiceName(),
                (ReplicationEventObject) wanReplicationEvent.getEventObject());
        response = true;
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new EWRPutBackupOperation(wanReplicationName, targetName, event);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(event);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        event = in.readData();
    }

    @Override
    public int getId() {
        return EWRDataSerializerHook.EWR_PUT_OPERATION;
    }
}
