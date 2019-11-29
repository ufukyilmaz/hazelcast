package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.internal.cluster.impl.operations.JoinOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;

/**
 * Retrieves the cluster state of a member.
 */
public class GetClusterStateOperation extends Operation implements JoinOperation {

    private ClusterState response;

    public GetClusterStateOperation() {
    }

    @Override
    public void run() {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        response = nodeEngine.getClusterService().getClusterState();
    }

    @Override
    public ClusterState getResponse() {
        return response;
    }

    @Override
    public int getFactoryId() {
        return HotRestartClusterSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return HotRestartClusterSerializerHook.GET_CLUSTER_STATE;
    }
}
