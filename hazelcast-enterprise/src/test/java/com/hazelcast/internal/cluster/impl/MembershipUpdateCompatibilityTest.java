package com.hazelcast.internal.cluster.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cluster.Member;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.CompatibilityTest;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.version.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.internal.cluster.impl.MembershipUpdateTest.assertMemberViewsAreSame;
import static com.hazelcast.internal.cluster.impl.MembershipUpdateTest.getMemberMap;
import static com.hazelcast.spi.properties.GroupProperty.TCP_JOIN_PORT_TRY_COUNT;
import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.getKnownPreviousVersionsCount;
import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.getOldestKnownVersion;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class MembershipUpdateCompatibilityTest extends HazelcastTestSupport {

    private CompatibilityTestHazelcastInstanceFactory factory;

    @Before
    public void init() {
        factory = new CompatibilityTestHazelcastInstanceFactory();
    }

    @After
    public void shutdown() {
        factory.terminateAll();
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void sequential_member_join_withTCP() {
        sequential_member_join(false);
    }

    @Test
    public void sequential_member_join_withMulticast() {
        sequential_member_join(true);
    }

    private void sequential_member_join(boolean multicastEnabled) {
        HazelcastInstance[] instances = factory.newInstances(createConfig(multicastEnabled));

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(instances.length, instance);
        }

        Cluster referenceCluster = instances[0].getCluster();
        for (HazelcastInstance instance : instances) {
            Cluster cluster = instance.getCluster();
            assertClustersAreSame(referenceCluster, cluster);
        }
    }

    @Test
    public void parallel_member_join_withTCP() {
        parallel_member_join(false);
    }

    @Test
    public void parallel_member_join_withMulticast() {
        parallel_member_join(true);
    }

    private void parallel_member_join(final boolean multicastEnabled) {
        final AtomicReferenceArray<HazelcastInstance> instances
                = new AtomicReferenceArray<HazelcastInstance>(getKnownPreviousVersionsCount() + 1);

        // start a member with older version to prevent a member with latest codebase version being master
        instances.set(0, factory.newHazelcastInstance(createConfig(multicastEnabled)));

        for (int i = 1; i < instances.length(); i++) {
            final int ix = i;
            spawn(new Runnable() {
                @Override
                public void run() {
                    instances.set(ix, factory.newHazelcastInstance(createConfig(multicastEnabled)));
                }
            });
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = 0; i < instances.length(); i++) {
                    HazelcastInstance instance = instances.get(i);
                    assertNotNull(instance);
                    assertClusterSize(instances.length(), instance);
                }
            }
        });

        Cluster referenceCluster = instances.get(0).getCluster();
        for (int i = 0; i < instances.length(); i++) {
            Cluster cluster = instances.get(i).getCluster();
            assertClustersAreSame(referenceCluster, cluster);
        }
    }

    @Test
    public void latestVersionSlave_terminates() {
        HazelcastInstance[] instances = factory.newInstances(createConfig(false));
        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(instances.length, instance);
        }

        instances[instances.length - 1].getLifecycleService().terminate();

        for (int i = 0; i < instances.length - 1; i++) {
            assertClusterSizeEventually(instances.length - 1, instances[i]);
        }
    }

    @Test
    public void previousVersionMaster_terminates() {
        HazelcastInstance[] instances = factory.newInstances(createConfig(false));
        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(instances.length, instance);
        }

        instances[0].getLifecycleService().terminate();

        for (int i = 1; i < instances.length; i++) {
            assertClusterSizeEventually(instances.length - 1, instances[i]);
        }
    }

    @Test
    public void latestVersionMaster_terminates() {
        HazelcastInstance master = HazelcastInstanceFactory.newHazelcastInstance(createConfig(false));
        master.getCluster().changeClusterVersion(Version.of(getOldestKnownVersion()));

        HazelcastInstance[] instances = factory.newInstances(createConfig(false), getKnownPreviousVersionsCount());

        assertClusterSize(instances.length + 1, master);
        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(instances.length + 1, instance);
        }

        master.getLifecycleService().terminate();

        Cluster referenceCluster = instances[0].getCluster();
        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(instances.length, instance);
            assertClustersAreSame(referenceCluster, instance.getCluster());
        }
    }

    @Test
    public void previousVersionSlave_terminates() {
        HazelcastInstance master = HazelcastInstanceFactory.newHazelcastInstance(createConfig(false));
        master.getCluster().changeClusterVersion(Version.of(getOldestKnownVersion()));

        HazelcastInstance[] instances = factory.newInstances(createConfig(false), getKnownPreviousVersionsCount());

        assertClusterSize(instances.length + 1, master);
        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(instances.length + 1, instance);
        }

        instances[0].getLifecycleService().terminate();

        assertClusterSizeEventually(instances.length, master);
        Cluster referenceCluster = master.getCluster();

        for (int i = 1; i < instances.length; i++) {
            assertClusterSizeEventually(instances.length, instances[i]);
            assertClustersAreSame(referenceCluster, instances[i].getCluster());
        }
    }

    @Test
    public void restartAllMembers_whenPartitionsAssigned() {
        HazelcastInstance[] instances = factory.newInstances(createConfig(false));
        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(instances.length, instance);
        }
        warmUpPartitions(instances);

        LinkedList<HazelcastInstance> instanceList = new LinkedList<HazelcastInstance>(Arrays.asList(instances));
        for (int ix = 0; ix < instances.length; ix++) {
            instanceList.removeFirst().shutdown();

            for (HazelcastInstance instance : instanceList) {
                assertClusterSizeEventually(instances.length - 1, instance);
            }

            Cluster referenceCluster = instanceList.getFirst().getCluster();
            for (HazelcastInstance instance : instanceList) {
                Cluster cluster = instance.getCluster();
                assertClustersAreSame(referenceCluster, cluster);
            }

            instanceList.add(factory.newHazelcastInstance(createConfig(false)));
            for (HazelcastInstance instance : instanceList) {
                assertClusterSizeEventually(instances.length, instance);
            }

            referenceCluster = instanceList.getFirst().getCluster();
            for (HazelcastInstance instance : instanceList) {
                Cluster cluster = instance.getCluster();
                assertClustersAreSame(referenceCluster, cluster);
            }
        }
    }

    @Test
    public void previousVersionMembers_joinToLatestVersionMaster_withTCP() {
        previousVersionMembers_joinToLatestVersionMaster(false);
    }

    @Test
    public void previousVersionMembers_joinToLatestVersionMaster_withMulticast() {
        previousVersionMembers_joinToLatestVersionMaster(true);
    }

    private void previousVersionMembers_joinToLatestVersionMaster(boolean multicastEnabled) {
        HazelcastInstance master = HazelcastInstanceFactory.newHazelcastInstance(createConfig(multicastEnabled));
        master.getCluster().changeClusterVersion(Version.of(getOldestKnownVersion()));

        Config config = createConfig(multicastEnabled);
        HazelcastInstance[] instances = factory.newInstances(config, getKnownPreviousVersionsCount());

        assertClusterSize(instances.length + 1, master);

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(instances.length + 1, instance);
        }

        Cluster referenceCluster = master.getCluster();
        for (HazelcastInstance instance : instances) {
            assertClustersAreSame(referenceCluster, instance.getCluster());
        }
    }

    @Test
    public void upgradeCluster_whenAllOlderVersionMembersLeft() {
        HazelcastInstance[] instances = factory.newInstances(createConfig(false), getKnownPreviousVersionsCount() + 2);
        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(instances.length, instance);
        }

        for (int i = 0; i < getKnownPreviousVersionsCount(); i++) {
            instances[i].shutdown();
        }

        final HazelcastInstance instance1 = instances[instances.length - 1];
        final HazelcastInstance instance2 = instances[instances.length - 2];
        assertClusterSizeEventually(2, instance1, instance2);

        final Version currentVersion = getNode(instance1).getVersion().asVersion();
        changeClusterVersionEventually(instance1, currentVersion);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(currentVersion, instance2.getCluster().getClusterVersion());
            }
        });

        assertMemberViewsAreSame(getMemberMap(instance1), getMemberMap(instance2));
    }

    public static void changeClusterVersionEventually(HazelcastInstance hz, Version version) {
        final Cluster cluster = hz.getCluster();
        long timeout = TimeUnit.SECONDS.toMillis(ASSERT_TRUE_EVENTUALLY_TIMEOUT);
        Throwable t = null;
        while (timeout > 0) {
            long start = Clock.currentTimeMillis();
            try {
                cluster.changeClusterVersion(version);
                return;
            } catch (Throwable e) {
                t = e;
            }
            sleepMillis(500);
            long end = Clock.currentTimeMillis();
            timeout -= (end - start);
        }
        throw rethrow(t);
    }

    private Config createConfig(boolean multicastEnabled) {
        Config config = new Config();
        config.setProperty(TCP_JOIN_PORT_TRY_COUNT.getName(), String.valueOf(getKnownPreviousVersionsCount()));
        if (!multicastEnabled) {
            JoinConfig join = config.getNetworkConfig().getJoin();
            join.getMulticastConfig().setEnabled(false);
            join.getTcpIpConfig().setEnabled(true).clear().addMember("127.0.0.1");
        }
        return config;
    }

    private static void assertClustersAreSame(Cluster referenceCluster, Cluster cluster) {
        Member[] refMembers = referenceCluster.getMembers().toArray(new Member[0]);
        Member[] members = cluster.getMembers().toArray(new Member[0]);

        assertEquals(refMembers.length, members.length);
        for (int i = 0; i < refMembers.length; i++) {
            Member refMember = refMembers[i];
            Member member = members[i];

            assertMembersAreSame(refMember, member);
        }
    }

    private static void assertMembersAreSame(Member refMember, Member member) {
        assertEquals(refMember.getUuid(), member.getUuid());
        assertEquals(refMember.getAddress(), member.getAddress());
        assertEquals(refMember.isLiteMember(), member.isLiteMember());
    }
}
