package com.hazelcast.quorum;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MembershipAdapter;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.Node;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.tcp.FirewallingMockConnectionManager;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.generateRandomString;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NativeMemPartitionedCluster {
    protected TestHazelcastInstanceFactory factory;
    public HazelcastInstance h1;
    public HazelcastInstance h2;
    public HazelcastInstance h3;
    public HazelcastInstance h4;
    public HazelcastInstance h5;

    public NativeMemPartitionedCluster(TestHazelcastInstanceFactory factory) {
        this.factory = factory;
    }

    public NativeMemPartitionedCluster partitionFiveMembersThreeAndTwo(CacheSimpleConfig cacheSimpleConfig, QuorumConfig quorumConfig) throws InterruptedException {
        createFiveMemberCluster(cacheSimpleConfig, quorumConfig);
        return splitFiveMembersThreeAndTwo();
    }

    public NativeMemPartitionedCluster createFiveMemberCluster(CacheSimpleConfig cacheSimpleConfig, QuorumConfig quorumConfig) {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "9999");
        config.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "9999");
        config.getGroupConfig().setName(generateRandomString(10));
        config.addCacheConfig(cacheSimpleConfig);
        config.addQuorumConfig(quorumConfig);
        config.setNativeMemoryConfig(new NativeMemoryConfig().setEnabled(true).setSize(new MemorySize(256, MemoryUnit.MEGABYTES)));
        createInstances(config);

        return this;
    }

    public NativeMemPartitionedCluster splitFiveMembersThreeAndTwo() throws InterruptedException {
        final CountDownLatch splitLatch = new CountDownLatch(6);
        h4.getCluster().addMembershipListener(new MembershipAdapter() {
            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                splitLatch.countDown();
            }
        });
        h5.getCluster().addMembershipListener(new MembershipAdapter() {
            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                splitLatch.countDown();
            }
        });

        splitCluster();

        assertTrue(splitLatch.await(30, TimeUnit.SECONDS));
        assertEquals(3, h1.getCluster().getMembers().size());
        assertEquals(3, h2.getCluster().getMembers().size());
        assertEquals(3, h3.getCluster().getMembers().size());
        assertEquals(2, h4.getCluster().getMembers().size());
        assertEquals(2, h5.getCluster().getMembers().size());
        return this;
    }

    private void createInstances(Config config) {
        h1 = factory.newHazelcastInstance(config);
        h2 = factory.newHazelcastInstance(config);
        h3 = factory.newHazelcastInstance(config);
        h4 = factory.newHazelcastInstance(config);
        h5 = factory.newHazelcastInstance(config);
    }

    private void splitCluster() {
        Node n1 = getNode(h1);
        Node n2 = getNode(h2);
        Node n3 = getNode(h3);
        Node n4 = getNode(h4);
        Node n5 = getNode(h5);

        FirewallingMockConnectionManager cm1 = getConnectionManager(n1);
        FirewallingMockConnectionManager cm2 = getConnectionManager(n2);
        FirewallingMockConnectionManager cm3 = getConnectionManager(n3);
        FirewallingMockConnectionManager cm4 = getConnectionManager(n4);
        FirewallingMockConnectionManager cm5 = getConnectionManager(n5);

        cm1.block(n4.address);
        cm2.block(n4.address);
        cm3.block(n4.address);

        cm1.block(n5.address);
        cm2.block(n5.address);
        cm3.block(n5.address);

        cm4.block(n1.address);
        cm4.block(n2.address);
        cm4.block(n3.address);

        cm5.block(n1.address);
        cm5.block(n2.address);
        cm5.block(n3.address);

        n4.clusterService.removeAddress(n1.address);
        n4.clusterService.removeAddress(n2.address);
        n4.clusterService.removeAddress(n3.address);

        n5.clusterService.removeAddress(n1.address);
        n5.clusterService.removeAddress(n2.address);
        n5.clusterService.removeAddress(n3.address);

        n1.clusterService.removeAddress(n4.address);
        n2.clusterService.removeAddress(n4.address);
        n3.clusterService.removeAddress(n4.address);

        n1.clusterService.removeAddress(n5.address);
        n2.clusterService.removeAddress(n5.address);
        n3.clusterService.removeAddress(n5.address);
    }

    private static FirewallingMockConnectionManager getConnectionManager(Node node) {
        return (FirewallingMockConnectionManager) node.getConnectionManager();
    }
}
