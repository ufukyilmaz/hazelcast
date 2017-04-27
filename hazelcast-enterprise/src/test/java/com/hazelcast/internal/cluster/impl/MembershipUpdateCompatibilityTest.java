package com.hazelcast.internal.cluster.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category(CompatibilityTest.class)
public class MembershipUpdateCompatibilityTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "multicast-{0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{true, false});
    }

    @Parameterized.Parameter
    public boolean multicastEnabled;

    private CompatibilityTestHazelcastInstanceFactory factory;

    @Before
    public void init() {
        factory = new CompatibilityTestHazelcastInstanceFactory();
    }

    @After
    public void shutdown() {
        factory.terminateAll();
    }

    @Test
    public void sequential_member_join() {
        HazelcastInstance[] instances = factory.newInstances(createConfig());

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(instances.length, instance);
        }

        Cluster referenceCluster = instances[0].getCluster();
        for (HazelcastInstance instance : instances) {
            Cluster cluster = instance.getCluster();
            assertClustersAreSame(referenceCluster, cluster);
        }
    }

    private Config createConfig() {
        Config config = new Config();
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
