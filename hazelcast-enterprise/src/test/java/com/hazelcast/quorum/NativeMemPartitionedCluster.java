package com.hazelcast.quorum;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MembershipAdapter;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.spi.properties.GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.closeConnectionBetween;
import static com.hazelcast.test.HazelcastTestSupport.generateRandomString;
import static com.hazelcast.test.SplitBrainTestSupport.blockCommunicationBetween;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NativeMemPartitionedCluster {
    public static final String QUORUM_ID = "threeNodeQuorumRule";
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
        return splitFiveMembersThreeAndTwo(quorumConfig.getName());
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

    public NativeMemPartitionedCluster splitFiveMembersThreeAndTwo(String quorumId) throws InterruptedException {
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
        verifyQuorums(SUCCESSFUL_SPLIT_TEST_QUORUM_NAME);
        verifyQuorums(quorumId);
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
        blockCommunicationBetween(h1, h4);
        blockCommunicationBetween(h1, h5);

        blockCommunicationBetween(h2, h4);
        blockCommunicationBetween(h2, h5);

        blockCommunicationBetween(h3, h4);
        blockCommunicationBetween(h3, h5);

        closeConnectionBetween(h4, h3);
        closeConnectionBetween(h4, h2);
        closeConnectionBetween(h4, h1);

        closeConnectionBetween(h5, h3);
        closeConnectionBetween(h5, h2);
        closeConnectionBetween(h5, h1);
    }

    private void verifyQuorums(String quorumId) {
        assertQuorumIsPresentEventually(h1, quorumId);
        assertQuorumIsPresentEventually(h2, quorumId);
        assertQuorumIsPresentEventually(h3, quorumId);
        assertQuorumIsAbsentEventually(h4, quorumId);
        assertQuorumIsAbsentEventually(h5, quorumId);
    }

    private void assertQuorumIsPresentEventually(final HazelcastInstance instance, final String quorumId) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(instance.getQuorumService().getQuorum(quorumId).isPresent());
            }
        });
    }

    private void assertQuorumIsAbsentEventually(final HazelcastInstance instance, final String quorumId) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertFalse(instance.getQuorumService().getQuorum(quorumId).isPresent());
            }
        });
    }

}
