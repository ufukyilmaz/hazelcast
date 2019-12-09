package com.hazelcast.enterprise.wan.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Response for WAN protocol negotiation from a WAN replication target.
 */
public class WanProtocolNegotiationResponse implements IdentifiedDataSerializable {
    private WanProtocolNegotiationStatus status;
    private MemberVersion memberVersion;
    private Version clusterVersion;
    private Version chosenWanProtocolVersion;
    private Map<String, String> metadata;

    @SuppressWarnings("unused")
    public WanProtocolNegotiationResponse() {
    }

    public WanProtocolNegotiationResponse(WanProtocolNegotiationStatus status,
                                          MemberVersion memberVersion,
                                          Version clusterVersion,
                                          Version chosenWanProtocolVersion,
                                          Map<String, String> metadata) {
        this.status = status;
        this.memberVersion = memberVersion;
        this.clusterVersion = clusterVersion;
        this.chosenWanProtocolVersion = chosenWanProtocolVersion;
        this.metadata = metadata;
    }

    /**
     * Returns the protocol negotiation status.
     */
    public WanProtocolNegotiationStatus getStatus() {
        return status;
    }

    /**
     * Returns the chosen protocol version.
     */
    public Version getChosenWanProtocolVersion() {
        return chosenWanProtocolVersion;
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.WAN_PROTOCOL_NEGOTIATION_RESPONSE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByte(status.getStatusCode());
        out.writeObject(memberVersion);
        out.writeObject(clusterVersion);
        out.writeObject(chosenWanProtocolVersion);
        out.writeInt(metadata.size());
        for (Entry<String, String> entry : metadata.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        status = WanProtocolNegotiationStatus.getByType(in.readByte());
        memberVersion = in.readObject();
        clusterVersion = in.readObject();
        chosenWanProtocolVersion = in.readObject();
        int metadataSize = in.readInt();
        metadata = MapUtil.createHashMap(metadataSize);
        for (int i = 0; i < metadataSize; i++) {
            metadata.put(in.readUTF(), in.readUTF());
        }
    }
}
