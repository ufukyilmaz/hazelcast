package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.SerializationSamplesExcluded;
import com.hazelcast.version.MemberVersion;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;

/**
 * Creates a cluster configured with multicast joiner, then change cluster version.
 */
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({NightlyTest.class, SerializationSamplesExcluded.class})
public class MulticastJoinerClusterUpgradeTest extends AbstractClusterUpgradeTest {

    @Override
    HazelcastInstance createHazelcastInstance(MemberVersion version, Config config) {
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, version.toString());
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        System.clearProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
        return hz;
    }

    @Override
    void upgradeInstances(final HazelcastInstance[] instances, MemberVersion version, Config config) {
        try {
            // upgrade one by one each member of the cluster to the next version
            for (int i = 0; i < instances.length; i++) {
                instances[i].shutdown();
                waitAllForSafeState(instances);
                // if new node's version is incompatible, then node startup will fail with IllegalStateException
                instances[i] = createHazelcastInstance(version, config);
                waitAllForSafeState(instances);
                // assert all members are in the cluster
                assertClusterSizeEventually(instances.length, instances[0], 15);
            }
        } finally {
            System.clearProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
        }
    }

    @Override
    void assertNodesVersion(MemberVersion version) {
        for (int i = 0; i < CLUSTER_MEMBERS_COUNT; i++) {
            assertEquals(version, getNode(clusterMembers[i]).getVersion());
        }
    }

    @Override
    protected Config getConfig() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        return config;
    }
}
