package com.hazelcast.enterprise.wan.impl.operation;

import com.hazelcast.config.GroupConfig;
import com.hazelcast.internal.cluster.impl.operations.JoinOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.hazelcast.enterprise.wan.impl.operation.WanProtocolNegotiationStatus.GROUP_NAME_MISMATCH;
import static com.hazelcast.enterprise.wan.impl.operation.WanProtocolNegotiationStatus.OK;
import static com.hazelcast.enterprise.wan.impl.operation.WanProtocolNegotiationStatus.PROTOCOL_MISMATCH;

/**
 * WAN protocol negotiation operation sent from source to target cluster to
 * negotiate WAN protocol and return metadata about target member and
 * cluster.
 */
public class WanProtocolNegotiationOperation extends Operation implements JoinOperation {
    private List<Version> sourceWanProtocolVersions;
    private String sourceGroupName;
    private String targetGroupName;
    private WanProtocolNegotiationResponse response;

    public WanProtocolNegotiationOperation() {
    }

    public WanProtocolNegotiationOperation(String sourceGroupName,
                                           String targetGroupName,
                                           List<Version> sourceWanProtocolVersions) {
        this.sourceGroupName = sourceGroupName;
        this.targetGroupName = targetGroupName;
        this.sourceWanProtocolVersions = sourceWanProtocolVersions;
    }

    @Override
    public void run() {
        MemberVersion memberVersion = getNodeEngine().getLocalMember().getVersion();
        Version clusterVersion = getNodeEngine().getClusterService().getClusterVersion();
        Map<String, String> metadata = Collections.emptyMap();

        GroupConfig groupConfig = getNodeEngine().getConfig().getGroupConfig();
        if (!targetGroupName.equals(groupConfig.getName())) {
            String failureCause = String.format(
                    "WAN protocol negotiation from (%s,%s) failed because of group name mismatch. ",
                    sourceGroupName, getCallerAddress());
            getLogger().info(failureCause);
            response = new WanProtocolNegotiationResponse(GROUP_NAME_MISMATCH, memberVersion, clusterVersion, null, metadata);
            return;
        }

        List<Version> localProtocolVersions = getNodeEngine().getWanReplicationService()
                                                             .getSupportedWanProtocolVersions();
        Optional<Version> chosenProtocolVersion = localProtocolVersions.stream()
                                                                       .filter(sourceWanProtocolVersions::contains)
                                                                       .findFirst();

        if (!chosenProtocolVersion.isPresent()) {
            String failureCause = String.format(
                    "WAN protocol negotiation from (%s , %s) failed because no matching WAN protocol versions were found. "
                            + "Source member supports %s, target supports %s",
                    sourceGroupName, getCallerAddress(), sourceWanProtocolVersions, localProtocolVersions);
            getLogger().info(failureCause);
            response = new WanProtocolNegotiationResponse(PROTOCOL_MISMATCH, memberVersion, clusterVersion, null, metadata);
            return;
        }

        response = new WanProtocolNegotiationResponse(OK, memberVersion, clusterVersion, chosenProtocolVersion.get(), metadata);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        sourceGroupName = in.readUTF();
        targetGroupName = in.readUTF();
        int sourceWanProtocolVersionSize = in.readInt();
        sourceWanProtocolVersions = new ArrayList<>(sourceWanProtocolVersionSize);
        for (int i = 0; i < sourceWanProtocolVersionSize; i++) {
            sourceWanProtocolVersions.add(in.readObject());
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(sourceGroupName);
        out.writeUTF(targetGroupName);
        out.writeInt(sourceWanProtocolVersions.size());
        for (Version version : sourceWanProtocolVersions) {
            out.writeObject(version);
        }
    }


    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return EWRDataSerializerHook.WAN_PROTOCOL_NEGOTIATION_OPERATION;
    }
}
