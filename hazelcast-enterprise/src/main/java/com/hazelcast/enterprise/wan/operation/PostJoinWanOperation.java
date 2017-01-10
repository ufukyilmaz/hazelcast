package com.hazelcast.enterprise.wan.operation;

import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Copies {@link WanReplicationConfig}s to joining member.
 * Existing configs are skipped.
 */
public class PostJoinWanOperation extends Operation implements IdentifiedDataSerializable {

    private List<WanReplicationConfig> wanReplicationConfigs = new ArrayList<WanReplicationConfig>();

    public PostJoinWanOperation() {
    }

    public PostJoinWanOperation(List<WanReplicationConfig> wanReplicationConfigs) {
        this.wanReplicationConfigs = wanReplicationConfigs;
    }

    public void addWanConfig(WanReplicationConfig wanConfig) {
        wanReplicationConfigs.add(wanConfig);
    }

    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EWRDataSerializerHook.POST_JOIN_WAN_OPERATION;
    }

    @Override
    public void run() throws Exception {
        EnterpriseWanReplicationService service = getService();
        for (WanReplicationConfig wanReplicationConfig : wanReplicationConfigs) {
            service.addWanReplicationConfigIfAbsent(wanReplicationConfig);
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
