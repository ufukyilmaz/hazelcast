package com.hazelcast.wan.map;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.WanReplicationPublisherDelegate;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.ENDPOINTS;
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

        final WanReplicationEndpoint endpoint = getWanReplicationDelegate(setupName).getEndpoint("B");

        createDataIn(clusterA, "map", 1, 10);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(endpoint.getStats().isConnected());
            }
        });
    }

    @Test
    public void testIsConnectionFalseWhenMisconfigured() {
        String setupName = "atob";
        setupReplicateFrom(configA, configB, clusterB.length, setupName, PassThroughMergePolicy.class.getName());

        for (WanPublisherConfig publisherConfig : configA.getWanReplicationConfig(setupName).getWanPublisherConfigs()) {
            final Map<String, Comparable> properties = publisherConfig.getProperties();
            final String endpoints = (String) properties.get(ENDPOINTS.key());
            final String endpointsWithError = endpoints.replaceAll(":58", ":59");
            properties.put(ENDPOINTS.key(), endpointsWithError);
        }

        startClusterA();
        startClusterB();

        final WanReplicationEndpoint endpoint = getWanReplicationDelegate(setupName).getEndpoint("B");

        createDataIn(clusterA, "map", 1, 10);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(endpoint.getStats().isConnected());
            }
        }, 10);
    }

    private WanReplicationPublisherDelegate getWanReplicationDelegate(String publisherName) {
        EnterpriseWanReplicationService wanReplicationService
                = (EnterpriseWanReplicationService) getNode(clusterA[0]).nodeEngine.getWanReplicationService();
        return (WanReplicationPublisherDelegate) wanReplicationService.getWanReplicationPublisher(publisherName);
    }

    @Override
    public String getReplicationImpl() {
        return WanBatchReplication.class.getName();
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.OBJECT;
    }
}
