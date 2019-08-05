package com.hazelcast.wan.fw;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.impl.WanReplicationPublisherDelegate;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchReplication;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.wan.DistributedServiceWanEventCounters.DistributedObjectWanEventCounters;
import com.hazelcast.wan.impl.WanSyncStatus;

import java.util.Map;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class WanTestSupport {
    public static EnterpriseWanReplicationService wanReplicationService(HazelcastInstance instance) {
        checkNotNull(instance, "Parameter instance should not be null");

        NodeEngine nodeEngine = TestUtil.getNode(instance).nodeEngine;
        return (EnterpriseWanReplicationService) nodeEngine.getWanReplicationService();
    }

    public static WanBatchReplication wanReplicationEndpoint(HazelcastInstance instance, WanReplication wanReplication) {
        WanReplicationPublisherDelegate delegate = (WanReplicationPublisherDelegate) wanReplicationService(instance)
                .getWanReplicationPublisher(wanReplication.getSetupName());
        return (WanBatchReplication) delegate.getEndpoint(wanReplication.getTargetClusterName());
    }

    public static void waitForSyncToComplete(final Cluster cluster) {
        assertTrueEventually(() -> {
            boolean syncFinished = true;
            for (HazelcastInstance instance : cluster.getMembers()) {
                syncFinished &= wanReplicationService(instance).getWanSyncState().getStatus() == WanSyncStatus.READY;
            }
            assertTrue(syncFinished);
        });
    }

    public static void waitForReplicationToStart(final Cluster sourceCluster, final Cluster targetCluster,
                                                 final WanReplication wanReplication, final String mapName) {
        assertTrueEventually(() -> {
            for (HazelcastInstance instance : sourceCluster.getMembers()) {
                Map<String, LocalWanStats> stats = wanReplicationService(instance).getStats();
                Map<String, DistributedObjectWanEventCounters> allMapEventCounters = stats
                        .get(wanReplication.getSetupName())
                        .getLocalWanPublisherStats()
                        .get(targetCluster.getName())
                        .getSentMapEventCounter();
                DistributedObjectWanEventCounters mapCounters = allMapEventCounters.get(mapName);
                assertNotNull(mapCounters);
                long updateCount = mapCounters.getUpdateCount();
                assertNotEquals(0, updateCount);
            }
        });
    }

}
