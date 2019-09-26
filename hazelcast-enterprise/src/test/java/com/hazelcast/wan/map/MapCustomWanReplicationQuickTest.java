package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
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

    private static final Class<?> CUSTOM_PUBLISHER = CustomWanPublisher.class;
    private static final Class<?> CUSTOM_CONSUMER = CustomWanConsumer.class;

    private HazelcastInstance[] sourceCluster = new HazelcastInstance[2];
    private HazelcastInstance[] targetCluster = new HazelcastInstance[2];

    @Test
    public void testCustomWanReplication() {
        setupReplicateFrom(configA, "customPublisherId",
                CUSTOM_PUBLISHER, "atob", PassThroughMergePolicy.class.getName(), null);
        initCluster(sourceCluster, configA);

        initializeConsumerConfig(configB);

        initCluster(targetCluster, configB);

        createDataIn(sourceCluster, "map", 0, 100);
        assertDataInFromEventually(targetCluster, "map", 0, 100, configA.getClusterName());
    }

    private void initializeConsumerConfig(Config config) {
        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig();
        wanReplicationConfig.setName("b");

        WanConsumerConfig consumerConfig = new WanConsumerConfig();
        consumerConfig.setClassName(CUSTOM_CONSUMER.getName());
        wanReplicationConfig.setWanConsumerConfig(consumerConfig);
        config.addWanReplicationConfig(wanReplicationConfig);
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.BINARY;
    }
}
