package com.hazelcast.enterprise.wan.impl.operation;

import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Copies {@link WanReplicationConfig}s to a joining member.
 * New WAN replication configurations are added to joining member and
 * existing WAN replication configurations are merged. This means that any
 * publishers that are not present on the joining member are added.
 */
public class PostJoinWanOperation extends Operation implements IdentifiedDataSerializable {

    private Collection<WanReplicationConfig> wanReplicationConfigs = new ArrayList<WanReplicationConfig>();

    public PostJoinWanOperation() {
    }

    public PostJoinWanOperation(Collection<WanReplicationConfig> wanReplicationConfigs) {
        this.wanReplicationConfigs = wanReplicationConfigs;
    }

    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return EWRDataSerializerHook.POST_JOIN_WAN_OPERATION;
    }

    @Override
    public void run() throws Exception {
        EnterpriseWanReplicationService service = getService();
        for (WanReplicationConfig wanReplicationConfig : wanReplicationConfigs) {
            service.appendWanReplicationConfig(wanReplicationConfig);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(wanReplicationConfigs.size());
        for (WanReplicationConfig wanReplicationConfig : wanReplicationConfigs) {
            out.writeObject(wanReplicationConfig);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            WanReplicationConfig wanReplicationConfig = in.readObject();
            wanReplicationConfigs.add(wanReplicationConfig);
        }
    }

    @Override
    public String getServiceName() {
        return EnterpriseWanReplicationService.SERVICE_NAME;
    }
}
