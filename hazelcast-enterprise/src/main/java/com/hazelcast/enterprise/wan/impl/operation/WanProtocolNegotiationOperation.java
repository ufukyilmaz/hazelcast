package com.hazelcast.enterprise.wan.impl.operation;

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

import static com.hazelcast.enterprise.wan.impl.operation.WanProtocolNegotiationStatus.CLUSTER_NAME_MISMATCH;
import static com.hazelcast.enterprise.wan.impl.operation.WanProtocolNegotiationStatus.OK;
import static com.hazelcast.enterprise.wan.impl.operation.WanProtocolNegotiationStatus.PROTOCOL_MISMATCH;

/**
 * WAN protocol negotiation operation sent from source to target cluster to
 * negotiate WAN protocol and return metadata about target member and
 * cluster.
 */
public class WanProtocolNegotiationOperation extends Operation implements JoinOperation {
    private List<Version> sourceWanProtocolVersions;
    private String sourceClusterName;
    private String targetClusterName;
    private WanProtocolNegotiationResponse response;

    public WanProtocolNegotiationOperation() {
    }

    public WanProtocolNegotiationOperation(String sourceClusterName,
                                           String targetClusterName,
                                           List<Version> sourceWanProtocolVersions) {
        this.sourceClusterName = sourceClusterName;
        this.targetClusterName = targetClusterName;
        this.sourceWanProtocolVersions = sourceWanProtocolVersions;
    }

    @Override
    public void run() {
        MemberVersion memberVersion = getNodeEngine().getLocalMember().getVersion();
        Version clusterVersion = getNodeEngine().getClusterService().getClusterVersion();
        Map<String, String> metadata = Collections.emptyMap();

        if (!targetClusterName.equals(getNodeEngine().getConfig().getClusterName())) {
            String failureCause = String.format(
                    "WAN protocol negotiation from (%s,%s) failed because of cluster name mismatch. ",
                    sourceClusterName, getCallerAddress());
            getLogger().info(failureCause);
            response = new WanProtocolNegotiationResponse(CLUSTER_NAME_MISMATCH, memberVersion, clusterVersion, null, metadata);
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
                    sourceClusterName, getCallerAddress(), sourceWanProtocolVersions, localProtocolVersions);
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
        sourceClusterName = in.readUTF();
        targetClusterName = in.readUTF();
        int sourceWanProtocolVersionSize = in.readInt();
        sourceWanProtocolVersions = new ArrayList<>(sourceWanProtocolVersionSize);
        for (int i = 0; i < sourceWanProtocolVersionSize; i++) {
            sourceWanProtocolVersions.add(in.readObject());
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(sourceClusterName);
        out.writeUTF(targetClusterName);
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
