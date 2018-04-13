package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.oldmembersupport.MapDataSerializerHookWithPostJoinMapOperation39;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.impl.MapDataSerializerHook;
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
import static com.hazelcast.internal.cluster.Versions.CURRENT_CLUSTER_VERSION;
import static com.hazelcast.internal.cluster.Versions.PREVIOUS_CLUSTER_VERSION;
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
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, PREVIOUS_CLUSTER_VERSION.toString());

        // we must use the overloaded classloader because one 3.10 member is "faking"
        // to be a 3.9 member by using version override. It must then send a 3.9 PostJoinMapOperation,
        // since other members know it is not a 3.9 member.
        // see: https://github.com/hazelcast/hazelcast-enterprise/issues/1997#issuecomment-377932234
        // RU_COMPAT_3_9
        final HazelcastInstance member1 = factory.newHazelcastInstance(configWithOverloadedClassLoader());

        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, CURRENT_CLUSTER_VERSION.toString());

        final HazelcastInstance member2 = factory.newHazelcastInstance();
        final HazelcastInstance member3 = factory.newHazelcastInstance();

        assertClusterSizeEventually(3, member2);

        member1.getLifecycleService().terminate();

        assertClusterSizeEventually(2, member2, member3);
        waitAllForSafeState(member2, member3);

        member2.getCluster().changeClusterVersion(CURRENT_CLUSTER_VERSION);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertJoinMemberListVersions(member2, member3);
            }
        });
    }

    private Config configWithOverloadedClassLoader() {
        return getConfig().setClassLoader(new ClassLoader() {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.equals(MapDataSerializerHook.class.getName())) {
                    return super.loadClass(MapDataSerializerHookWithPostJoinMapOperation39.class.getName());
                }
                return super.loadClass(name, resolve);
            }
        });
    }

}
