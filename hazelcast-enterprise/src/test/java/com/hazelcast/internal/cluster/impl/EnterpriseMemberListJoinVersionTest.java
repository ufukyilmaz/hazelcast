package com.hazelcast.internal.cluster.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.internal.cluster.Versions.V3_10;
import static com.hazelcast.internal.cluster.Versions.V3_9;
import static com.hazelcast.internal.cluster.impl.MemberListJoinVersionTest.assertJoinMemberListVersions;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterpriseMemberListJoinVersionTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    @Before
    public void init() {
        factory = createHazelcastInstanceFactory();
    }

    @Test
    public void when_clusterIsUpgradedTo310_then_allNodesReceiveJoinVersionsEventually() {
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, V3_9.toString());

        final HazelcastInstance member1 = factory.newHazelcastInstance();

        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, V3_10.toString());

        final HazelcastInstance member2 = factory.newHazelcastInstance();
        final HazelcastInstance member3 = factory.newHazelcastInstance();

        assertClusterSizeEventually(3, member2);

        member1.getLifecycleService().terminate();

        assertClusterSizeEventually(2, member2, member3);

        member2.getCluster().changeClusterVersion(V3_10);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertJoinMemberListVersions(member2, member3);
            }
        });
    }

}
