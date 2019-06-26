package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanConfigurationContextTest {

    @Test
    public void testDefaults() {
        final HashMap<String, Comparable> props = new HashMap<String, Comparable>();
        final WanPublisherConfig publisherConfig = new WanPublisherConfig();
        publisherConfig.setProperties(props);
        final WanConfigurationContext context = new WanConfigurationContext(publisherConfig);
        assertEquals(WanConfigurationContext.DEFAULT_IS_SNAPSHOT_ENABLED, context.isSnapshotEnabled());
        assertEquals(WanConfigurationContext.DEFAULT_BATCH_SIZE, context.getBatchSize());
        assertEquals(WanConfigurationContext.DEFAULT_BATCH_MAX_DELAY_MILLIS, context.getBatchMaxDelayMillis());
        assertEquals(WanConfigurationContext.DEFAULT_RESPONSE_TIMEOUT_MILLIS, context.getResponseTimeoutMillis());
        assertEquals(WanConfigurationContext.DEFAULT_ACKNOWLEDGE_TYPE, context.getAcknowledgeType().name());
        assertEquals(WanConfigurationContext.DEFAULT_USE_ENDPOINT_PRIVATE_ADDRESS, context.isUseEndpointPrivateAddress());
        assertEquals(WanConfigurationContext.DEFAULT_GROUP_PASS, context.getPassword());
        assertEquals(WanConfigurationContext.DEFAULT_MAX_ENDPOINTS, context.getMaxEndpoints());
        assertEquals(WanConfigurationContext.DEFAULT_DISCOVERY_TASK_PERIOD, context.getDiscoveryPeriodSeconds());
        assertEquals(WanConfigurationContext.DEFAULT_ENDPOINTS, context.getEndpoints());
    }

    @Test
    public void testOverridenValues() {
        final HashMap<String, Comparable> props = new HashMap<String, Comparable>();
        props.put(WanReplicationProperties.SNAPSHOT_ENABLED.key(), true);
        props.put(WanReplicationProperties.EXECUTOR_THREAD_COUNT.key(), 100);
        props.put(WanReplicationProperties.BATCH_SIZE.key(), 1);
        props.put(WanReplicationProperties.BATCH_MAX_DELAY_MILLIS.key(), 1);
        props.put(WanReplicationProperties.RESPONSE_TIMEOUT_MILLIS.key(), 1);
        props.put(WanReplicationProperties.ACK_TYPE.key(), WanAcknowledgeType.ACK_ON_RECEIPT);
        props.put(WanReplicationProperties.GROUP_PASSWORD.key(), "pass");
        props.put(WanReplicationProperties.DISCOVERY_USE_ENDPOINT_PRIVATE_ADDRESS.key(), true);
        props.put(WanReplicationProperties.DISCOVERY_PERIOD.key(), 1000);
        props.put(WanReplicationProperties.ENDPOINTS.key(), "A,B,C,D");


        final WanPublisherConfig publisherConfig = new WanPublisherConfig();
        publisherConfig.setProperties(props);
        final WanConfigurationContext context = new WanConfigurationContext(publisherConfig);

        assertTrue(context.isSnapshotEnabled());
        assertEquals(1, context.getBatchSize());
        assertEquals(1, context.getBatchMaxDelayMillis());
        assertEquals(1, context.getResponseTimeoutMillis());
        assertEquals(WanAcknowledgeType.ACK_ON_RECEIPT.name(), context.getAcknowledgeType().name());
        assertTrue(context.isUseEndpointPrivateAddress());
        assertEquals("pass", context.getPassword());
        assertEquals(1000, context.getDiscoveryPeriodSeconds());
        assertEquals("A,B,C,D", context.getEndpoints());
    }

    @Test
    public void testMaxEndpointsWhenEndpointsAreConfigured() {
        final HashMap<String, Comparable> props = new HashMap<String, Comparable>();
        props.put(WanReplicationProperties.MAX_ENDPOINTS.key(), 1);
        props.put(WanReplicationProperties.ENDPOINTS.key(), "A,B,C,D");

        final WanPublisherConfig publisherConfig = new WanPublisherConfig();
        publisherConfig.setProperties(props);
        final WanConfigurationContext context = new WanConfigurationContext(publisherConfig);

        assertEquals(Integer.MAX_VALUE, context.getMaxEndpoints());
        assertEquals("A,B,C,D", context.getEndpoints());
    }

    @Test
    public void testMaxEndpointsWhenEndpointsAreNotConfigured() {
        final HashMap<String, Comparable> props = new HashMap<String, Comparable>();
        props.put(WanReplicationProperties.MAX_ENDPOINTS.key(), 1);

        final WanPublisherConfig publisherConfig = new WanPublisherConfig();
        publisherConfig.setProperties(props);
        final WanConfigurationContext context = new WanConfigurationContext(publisherConfig);

        assertEquals(1, context.getMaxEndpoints());
        assertEquals("", context.getEndpoints());
    }
}
