package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.internal.cluster.impl.operations.JoinOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.hotrestart.HotRestartIntegrationService;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

/**
 * Operation which is sent to master by members to initiate force start process on master.
 */
public class TriggerForceStartOnMasterOperation extends Operation implements JoinOperation {

    private boolean partialStart;

    public TriggerForceStartOnMasterOperation() {
    }

    public TriggerForceStartOnMasterOperation(boolean partialStart) {
        this.partialStart = partialStart;
    }

    @Override
    public void run() throws Exception {
        Address caller = getCallerAddress();
        getLogger().warning("Received " + (partialStart ? "partial" : "force") +  " start request from: " + caller);

        HotRestartIntegrationService service = getService();
        ClusterMetadataManager clusterMetadataManager = service.getClusterMetadataManager();
        if (partialStart) {
            clusterMetadataManager.handlePartialStartRequest();
        } else {
            clusterMetadataManager.handleForceStartRequest();
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public String getServiceName() {
        return HotRestartIntegrationService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return HotRestartClusterSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return HotRestartClusterSerializerHook.TRIGGER_FORCE_START;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(partialStart);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        partialStart = in.readBoolean();
    }
}
