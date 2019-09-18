package com.hazelcast.enterprise.wan.impl.sync;

import com.hazelcast.enterprise.wan.impl.AbstractWanAntiEntropyEvent;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.impl.operation.EWRDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

/**
 * Operation responsible to publish WAN anti-entropy events.
 * <p>
 * This operation merely is concerned with publishing the event on all
 * partitions. Whether the event is fully processed when this method returns
 * depends on the semantics of processing each event type.
 * In case of WAN sync event, the sync is not complete when this method
 * returns. After this method returns, entries for all partitions have been
 * enqueued but not yet replicated.
 */
public class WanAntiEntropyEventPublishOperation extends Operation
        implements IdentifiedDataSerializable, AllowedDuringPassiveState {

    private String wanReplicationName;
    private String wanPublisherId;
    private AbstractWanAntiEntropyEvent event;

    public WanAntiEntropyEventPublishOperation() {
    }

    public WanAntiEntropyEventPublishOperation(String wanReplicationName,
                                               String wanPublisherId,
                                               AbstractWanAntiEntropyEvent event) {
        this.wanReplicationName = wanReplicationName;
        this.wanPublisherId = wanPublisherId;
        this.event = event;
    }

    @Override
    public void run() throws Exception {
        EnterpriseWanReplicationService wanReplicationService = getService();
        event.setOp(this);
        wanReplicationService.publishAntiEntropyEvent(wanReplicationName, wanPublisherId, event);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(wanReplicationName);
        out.writeUTF(wanPublisherId);
        out.writeObject(event);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        wanReplicationName = in.readUTF();
        wanPublisherId = in.readUTF();
        event = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return EWRDataSerializerHook.WAN_SYNC_OPERATION;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }
}
