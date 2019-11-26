package com.hazelcast.wan.map;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WanBatchReplicationPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchReplication;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.impl.DelegatingWanReplicationScheme;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanConnectedTest extends MapWanReplicationTestSupport {

    @Test
    public void testIsConnectionTrueWhenWellConfigured() {
        String setupName = "atob";
        setupReplicateFrom(configA, configB, clusterB.length, setupName, PassThroughMergePolicy.class.getName());

        startClusterA();
        startClusterB();

        WanBatchReplication publisher = (WanBatchReplication) getWanReplicationDelegate(setupName).getPublisher("B");

        createDataIn(clusterA, "map", 1, 10);

        assertTrueEventually(() -> assertTrue(publisher.getStats().isConnected()));
    }

    @Test
    public void testIsConnectionFalseWhenMisconfigured() {
        String setupName = "atob";
        setupReplicateFrom(configA, configB, clusterB.length, setupName, PassThroughMergePolicy.class.getName());

        WanReplicationConfig wanConfig = configA.getWanReplicationConfig(setupName);
        for (WanBatchReplicationPublisherConfig publisherConfig : wanConfig.getBatchPublisherConfigs()) {
            String endpoints = publisherConfig.getTargetEndpoints();
            String endpointsWithError = endpoints.replaceAll(":58", ":59");
            publisherConfig.setTargetEndpoints(endpointsWithError);
        }

        startClusterA();
        startClusterB();

        WanBatchReplication endpoint = (WanBatchReplication) getWanReplicationDelegate(setupName).getPublisher("B");

        createDataIn(clusterA, "map", 1, 10);

        assertTrueAllTheTime(() -> assertFalse(endpoint.getStats().isConnected()), 10);
    }

    private DelegatingWanReplicationScheme getWanReplicationDelegate(String publisherName) {
        EnterpriseWanReplicationService wanReplicationService
                = (EnterpriseWanReplicationService) getNode(clusterA[0]).nodeEngine.getWanReplicationService();
        return wanReplicationService.getWanReplicationPublishers(publisherName);
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.OBJECT;
    }
}
