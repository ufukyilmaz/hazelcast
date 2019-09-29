package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.internal.hotrestart.InternalHotRestartService;
import com.hazelcast.internal.cluster.impl.operations.JoinOperation;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static java.util.Collections.unmodifiableSet;

/**
 * Sends excluded member UUIDs to a member which is in that set.
 *
 * We need this operation because we don't allow an excluded member to join to the cluster.
 * Therefore, we notify it so that the excluded member can force-start itself.
 */
public class SendExcludedMemberUuidsOperation extends Operation implements JoinOperation {

    private Set<UUID> excludedMemberUuids;

    public SendExcludedMemberUuidsOperation() {
    }

    public SendExcludedMemberUuidsOperation(Set<UUID> excludedMemberUuids) {
        this.excludedMemberUuids = excludedMemberUuids != null ? excludedMemberUuids : Collections.emptySet();
    }

    @Override
    public void run() {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        InternalHotRestartService hotRestartService = nodeEngine.getNode().getNodeExtension().getInternalHotRestartService();
        hotRestartService.handleExcludedMemberUuids(getCallerAddress(), excludedMemberUuids);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(excludedMemberUuids.size());
        for (UUID uuid : excludedMemberUuids) {
            UUIDSerializationUtil.writeUUID(out, uuid);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        Set<UUID> excludedMemberUuids = new HashSet<>();
        for (int i = 0; i < size; i++) {
            excludedMemberUuids.add(UUIDSerializationUtil.readUUID(in));
        }
        this.excludedMemberUuids = unmodifiableSet(excludedMemberUuids);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public int getFactoryId() {
        return HotRestartClusterSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return HotRestartClusterSerializerHook.SEND_EXCLUDED_MEMBER_UUIDS;
    }
}
