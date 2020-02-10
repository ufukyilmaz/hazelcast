package com.hazelcast.wan.map;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchPublisher;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.impl.DelegatingWanScheme;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanBatchPublisherConnectedTest extends WanMapTestSupport {

    @Test
    public void testIsConnectionTrueWhenWellConfigured() {
        String setupName = "atob";
        setupReplicateFrom(configA, configB, clusterB.length, setupName, PassThroughMergePolicy.class.getName());

        startClusterA();
        startClusterB();

        WanBatchPublisher publisher = (WanBatchPublisher) getWanReplicationDelegate(setupName).getPublisher("B");

        createDataIn(clusterA, "map", 1, 10);

        assertTrueEventually(() -> assertTrue(publisher.getStats().isConnected()));
    }

    @Test
    public void testIsConnectionFalseWhenMisconfigured() {
        String setupName = "atob";
        setupReplicateFrom(configA, configB, clusterB.length, setupName, PassThroughMergePolicy.class.getName());

        WanReplicationConfig wanReplicationConfig = configA.getWanReplicationConfig(setupName);
        for (WanBatchPublisherConfig publisherConfig : wanReplicationConfig.getBatchPublisherConfigs()) {
            String endpoints = publisherConfig.getTargetEndpoints();
            String endpointsWithError = endpoints.replaceAll(":58", ":59");
            publisherConfig.setTargetEndpoints(endpointsWithError);
        }

        startClusterA();
        startClusterB();

        WanBatchPublisher endpoint = (WanBatchPublisher) getWanReplicationDelegate(setupName).getPublisher("B");

        createDataIn(clusterA, "map", 1, 10);

        assertTrueAllTheTime(() -> assertFalse(endpoint.getStats().isConnected()), 10);
    }

    private DelegatingWanScheme getWanReplicationDelegate(String publisherName) {
        EnterpriseWanReplicationService wanReplicationService
                = (EnterpriseWanReplicationService) Accessors.getNode(clusterA[0]).nodeEngine.getWanReplicationService();
        return wanReplicationService.getWanReplicationPublishers(publisherName);
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.OBJECT;
    }
}
