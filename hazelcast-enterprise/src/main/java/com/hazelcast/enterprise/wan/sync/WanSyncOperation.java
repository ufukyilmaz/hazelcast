package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * Operation that is responsible to publish {@link WanSyncEvent}s and trigger
 * WAN sync. The response for this operation is sent when events for all
 * entries have been enqueued for replication but not yet replicated.
 */
public class WanSyncOperation extends Operation implements IdentifiedDataSerializable {

    private String wanReplicationName;
    private String targetGroupName;
    private WanSyncEvent syncEvent;

    public WanSyncOperation() {
    }

    public WanSyncOperation(String wanReplicationName, String targetGroupName,
                            WanSyncEvent syncEvent) {
        this.wanReplicationName = wanReplicationName;
        this.targetGroupName = targetGroupName;
        this.syncEvent = syncEvent;
    }

    @Override
    public void run() throws Exception {
        EnterpriseWanReplicationService wanReplicationService = getService();
        syncEvent.setOp(this);
        wanReplicationService.publishSyncEvent(wanReplicationName, targetGroupName, syncEvent);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(wanReplicationName);
        out.writeUTF(targetGroupName);
        out.writeObject(syncEvent);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        wanReplicationName = in.readUTF();
        targetGroupName = in.readUTF();
        syncEvent = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EWRDataSerializerHook.WAN_SYNC_OPERATION;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }
}
