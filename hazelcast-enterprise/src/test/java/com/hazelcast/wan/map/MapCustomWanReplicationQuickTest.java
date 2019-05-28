package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.custom.CustomWanConsumer;
import com.hazelcast.wan.custom.CustomWanPublisher;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapCustomWanReplicationQuickTest extends MapWanReplicationTestSupport {

    private static final String CUSTOM_PUBLISHER = CustomWanPublisher.class.getName();
    private static final String CUSTOM_CONSUMER = CustomWanConsumer.class.getName();

    private HazelcastInstance[] sourceCluster = new HazelcastInstance[2];
    private HazelcastInstance[] targetCluster = new HazelcastInstance[2];

    @Test
    public void testCustomWanReplication() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        WanReplicationConfig wanConfig = configA.getWanReplicationConfig("atob");
        initializePublisherConfig(wanConfig);
        initCluster(sourceCluster, configA);

        initializeConsumerConfig(configB);

        initCluster(targetCluster, configB);

        createDataIn(sourceCluster, "map", 0, 100);
        assertDataInFromEventually(targetCluster, "map", 0, 100, configA.getGroupConfig().getName());
    }

    private void initializeConsumerConfig(Config config) {
        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig();
        wanReplicationConfig.setName("b");

        WanConsumerConfig consumerConfig = new WanConsumerConfig();
        consumerConfig.setClassName(CUSTOM_CONSUMER);
        wanReplicationConfig.setWanConsumerConfig(consumerConfig);
        config.addWanReplicationConfig(wanReplicationConfig);
    }

    private void initializePublisherConfig(WanReplicationConfig wanReplicationConfig) {
        WanPublisherConfig publisherConfig = new WanPublisherConfig();
        publisherConfig.setGroupName("targetGroup");
        publisherConfig.setClassName(CUSTOM_PUBLISHER);
        publisherConfig.setQueueCapacity(10000);
        publisherConfig.setQueueFullBehavior(WANQueueFullBehavior.DISCARD_AFTER_MUTATION);
        wanReplicationConfig.addWanPublisherConfig(publisherConfig);
    }

    @Override
    public String getReplicationImpl() {
        return CUSTOM_PUBLISHER;
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.BINARY;
    }
}
