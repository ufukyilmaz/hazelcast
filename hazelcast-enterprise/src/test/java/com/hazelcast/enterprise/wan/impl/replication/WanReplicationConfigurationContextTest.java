package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanReplicationConfigurationContextTest {

    @Test
    public void testDefaults() {
        WanConfigurationContext context = new WanConfigurationContext(new WanBatchPublisherConfig());
        assertFalse(context.isSnapshotEnabled());
        assertEquals(500, context.getBatchSize());
        assertEquals(1000, context.getBatchMaxDelayMillis());
        assertEquals(60000, context.getResponseTimeoutMillis());
        assertEquals(WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE, context.getAcknowledgeType());
        assertFalse(context.isUseEndpointPrivateAddress());
        assertEquals(Integer.MAX_VALUE, context.getMaxEndpoints());
        assertEquals(10, context.getDiscoveryPeriodSeconds());
        assertEquals("", context.getEndpoints());
    }

    @Test
    public void testOverridenValues() {
        WanBatchPublisherConfig publisherConfig = new WanBatchPublisherConfig()
                .setSnapshotEnabled(true)
                .setBatchSize(1)
                .setBatchMaxDelayMillis(1)
                .setResponseTimeoutMillis(1)
                .setAcknowledgeType(WanAcknowledgeType.ACK_ON_RECEIPT)
                .setUseEndpointPrivateAddress(true)
                .setDiscoveryPeriodSeconds(1000)
                .setTargetEndpoints("A,B,C,D");
        WanConfigurationContext context = new WanConfigurationContext(publisherConfig);

        assertTrue(context.isSnapshotEnabled());
        assertEquals(1, context.getBatchSize());
        assertEquals(1, context.getBatchMaxDelayMillis());
        assertEquals(1, context.getResponseTimeoutMillis());
        assertEquals(WanAcknowledgeType.ACK_ON_RECEIPT.name(), context.getAcknowledgeType().name());
        assertTrue(context.isUseEndpointPrivateAddress());
        assertEquals(1000, context.getDiscoveryPeriodSeconds());
        assertEquals("A,B,C,D", context.getEndpoints());
    }

    @Test
    public void testMaxEndpointsWhenEndpointsAreConfigured() {
        WanBatchPublisherConfig publisherConfig = new WanBatchPublisherConfig()
                .setMaxTargetEndpoints(1)
                .setTargetEndpoints("A,B,C,D");
        WanConfigurationContext context = new WanConfigurationContext(publisherConfig);

        assertEquals(Integer.MAX_VALUE, context.getMaxEndpoints());
        assertEquals("A,B,C,D", context.getEndpoints());
    }

    @Test
    public void testMaxEndpointsWhenEndpointsAreNotConfigured() {
        WanBatchPublisherConfig publisherConfig = new WanBatchPublisherConfig()
                .setMaxTargetEndpoints(1);
        final WanConfigurationContext context = new WanConfigurationContext(publisherConfig);

        assertEquals(1, context.getMaxEndpoints());
        assertEquals("", context.getEndpoints());
    }
}
