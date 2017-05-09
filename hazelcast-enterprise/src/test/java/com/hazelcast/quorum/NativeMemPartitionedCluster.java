package com.hazelcast.quorum;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MembershipAdapter;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.instance.Node;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.tcp.FirewallingConnectionManager;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.spi.properties.GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.generateRandomString;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.suspectMember;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NativeMemPartitionedCluster {
    private static final String SUCCESSFUL_SPLIT_TEST_QUORUM_NAME = "SUCCESSFULL_SPLIT_TEST_QUORUM";
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
        config.setProperty(MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "9999");
        config.setProperty(MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "9999");
        config.getGroupConfig().setName(generateRandomString(10));
        config.addCacheConfig(cacheSimpleConfig);
        config.addQuorumConfig(quorumConfig);
        config.setNativeMemoryConfig(new NativeMemoryConfig().setEnabled(true).setSize(new MemorySize(256, MemoryUnit.MEGABYTES)));
        config.addQuorumConfig(createSuccessfulSplitTestQuorum());
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
        assertClusterSizeEventually(3, h1, h2, h3);
        assertClusterSizeEventually(2, h4, h5);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertFalse(h4.getQuorumService().getQuorum(SUCCESSFUL_SPLIT_TEST_QUORUM_NAME).isPresent());
                assertFalse(h5.getQuorumService().getQuorum(SUCCESSFUL_SPLIT_TEST_QUORUM_NAME).isPresent());
            }
        });
        return this;
    }

    private QuorumConfig createSuccessfulSplitTestQuorum() {
        QuorumConfig splitConfig = new QuorumConfig();
        splitConfig.setEnabled(true);
        splitConfig.setSize(3);
        splitConfig.setName(SUCCESSFUL_SPLIT_TEST_QUORUM_NAME);
        return splitConfig;
    }

    private void createInstances(Config config) {
        h1 = factory.newHazelcastInstance(config);
        h2 = factory.newHazelcastInstance(config);
        h3 = factory.newHazelcastInstance(config);
        h4 = factory.newHazelcastInstance(config);
        h5 = factory.newHazelcastInstance(config);

        assertClusterSize(5, h1, h5);
        assertClusterSizeEventually(5, h2, h3, h4);
    }

    private void splitCluster() {
        Node n1 = getNode(h1);
        Node n2 = getNode(h2);
        Node n3 = getNode(h3);
        Node n4 = getNode(h4);
        Node n5 = getNode(h5);

        FirewallingConnectionManager cm1 = getConnectionManager(n1);
        FirewallingConnectionManager cm2 = getConnectionManager(n2);
        FirewallingConnectionManager cm3 = getConnectionManager(n3);
        FirewallingConnectionManager cm4 = getConnectionManager(n4);
        FirewallingConnectionManager cm5 = getConnectionManager(n5);

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

        suspectMember(n4, n1);
        suspectMember(n4, n2);
        suspectMember(n4, n3);

        suspectMember(n5, n1);
        suspectMember(n5, n2);
        suspectMember(n5, n3);

        suspectMember(n1, n4);
        suspectMember(n2, n4);
        suspectMember(n3, n4);

        suspectMember(n1, n5);
        suspectMember(n2, n5);
        suspectMember(n3, n5);
    }

    private static FirewallingConnectionManager getConnectionManager(Node node) {
        return (FirewallingConnectionManager) node.getConnectionManager();
    }
}
