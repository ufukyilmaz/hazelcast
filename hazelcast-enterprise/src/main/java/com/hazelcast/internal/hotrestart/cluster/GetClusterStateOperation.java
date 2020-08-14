package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;

/**
 * Retrieves the cluster state of a member.
 */
public class GetClusterStateOperation extends Operation implements UrgentSystemOperation,
        AllowedDuringPassiveState, IdentifiedDataSerializable {

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
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException || throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
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
