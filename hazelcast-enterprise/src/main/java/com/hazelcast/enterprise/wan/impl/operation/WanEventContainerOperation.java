package com.hazelcast.enterprise.wan.impl.operation;

import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.internal.cluster.impl.operations.WanOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.version.Version;

import java.io.IOException;

/**
 * Operation sent from the source WAN endpoint to the target endpoint.
 * This operation contains the changes in the source endpoint (source
 * cluster) and the acknowledge type. The acknowledge type defines when
 * the response for this operation will be sent to the source endpoint so
 * that it knows in which stage is the operation (accepted or completed).
 * <p>
 * The operation implements {@link AllowedDuringPassiveState} since its'
 * effects take place on a different cluster which may be
 * {@link com.hazelcast.cluster.ClusterState#ACTIVE} at the time when
 * it's invoked.
 */
public class WanEventContainerOperation extends Operation
        implements WanOperation, IdentifiedDataSerializable, AllowedDuringPassiveState {

    private IdentifiedDataSerializable event;
    private WanAcknowledgeType acknowledgeType;
    // can be written and read from different threads
    private final transient Version wanProtocolVersion;

    public WanEventContainerOperation() {
        wanProtocolVersion = Version.UNKNOWN;
    }

    public WanEventContainerOperation(IdentifiedDataSerializable event,
                                      WanAcknowledgeType acknowledgeType,
                                      Version wanProtocolVersion) {
        this.event = event;
        this.acknowledgeType = acknowledgeType;
        this.wanProtocolVersion = wanProtocolVersion;
    }

    /**
     * Sets the WAN protocol version which determines how serialisation will be
     * performed on the provided output stream.
     *
     * @param output the serialization output
     */
    private void setWanProtocolVersion(ObjectDataOutput output) {
        output.setWanProtocolVersion(wanProtocolVersion);
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
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.WAN_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        setWanProtocolVersion(out);
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
