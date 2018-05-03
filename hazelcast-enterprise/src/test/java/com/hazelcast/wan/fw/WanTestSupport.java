package com.hazelcast.wan.fw;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.WanReplicationPublisherDelegate;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.spi.NodeEngine;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class WanTestSupport {
    public static EnterpriseWanReplicationService wanReplicationService(HazelcastInstance instance) {
        checkNotNull(instance, "Parameter instance should not be null");

        NodeEngine nodeEngine = TestUtil.getNode(instance).nodeEngine;
        return (EnterpriseWanReplicationService) nodeEngine.getWanReplicationService();
    }

    public static void pauseWanReplication(Cluster cluster, String setupName, String targetGroupName) {
        for (HazelcastInstance instance : cluster.getMembers()) {
            if (instance != null) {
                wanReplicationService(instance).pause(setupName, targetGroupName);
            }
        }
    }

    public static void resumeWanReplication(Cluster cluster, String setupName, String targetGroupName) {
        for (HazelcastInstance instance : cluster.getMembers()) {
            wanReplicationService(instance).resume(setupName, targetGroupName);
        }
    }

    public static WanBatchReplication wanReplicationEndpoint(HazelcastInstance instance, WanReplication wanReplication) {
        WanReplicationPublisherDelegate delegate = (WanReplicationPublisherDelegate) wanReplicationService(instance)
                .getWanReplicationPublisher(wanReplication.getSetupName());
        return (WanBatchReplication) delegate.getEndpoint(wanReplication.getTargetClusterName());
    }

}
