package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cluster.AbstractClusterUpgradeTest.CLUSTER_MEMBERS_COUNT;
import static com.hazelcast.cluster.AbstractClusterUpgradeTest.CLUSTER_VERSION_CURRENT;
import static com.hazelcast.cluster.AbstractClusterUpgradeTest.CLUSTER_VERSION_NEXT_MINOR;
import static com.hazelcast.cluster.AbstractClusterUpgradeTest.MEMBER_VERSION_CURRENT;
import static com.hazelcast.cluster.AbstractClusterUpgradeTest.MEMBER_VERSION_NEXT_MINOR;
import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.internal.cluster.impl.ClusterVersionAutoUpgradeHelper.PROP_AUTO_UPGRADE_ENABLED;
import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.test.TestClusterUpgradeUtils.assertClusterVersion;
import static com.hazelcast.test.TestClusterUpgradeUtils.newHazelcastInstance;
import static com.hazelcast.test.TestClusterUpgradeUtils.upgradeClusterMembers;

/**
 * Keep this test as serial.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClusterVersionAutoUpgradeTest extends HazelcastTestSupport {

    private HazelcastInstance[] clusterMembers;
    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(CLUSTER_MEMBERS_COUNT);

    @Rule
    public final OverridePropertyRule overrideMemberVersionRule
            = set(HAZELCAST_INTERNAL_OVERRIDE_VERSION, MEMBER_VERSION_CURRENT.toString());

    @Before
    public void setUp() {
        Config config = getConfig();
        config.setProperty(PROP_AUTO_UPGRADE_ENABLED, "true");

        clusterMembers = new HazelcastInstance[CLUSTER_MEMBERS_COUNT];
        for (int i = 0; i < CLUSTER_MEMBERS_COUNT; i++) {
            clusterMembers[i] = newHazelcastInstance(factory, MEMBER_VERSION_CURRENT, config);
        }
    }

    @Test
    public void cluster_version_is_not_upgraded_when_auto_upgrade_disabled() {
        Config config = getConfig();
        config.setProperty(PROP_AUTO_UPGRADE_ENABLED, "false");

        upgradeClusterMembers(factory, clusterMembers, MEMBER_VERSION_NEXT_MINOR, config);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertClusterVersion(clusterMembers, CLUSTER_VERSION_CURRENT);
            }
        }, 5);
    }

    @Test
    public void cluster_version_upgraded_to_next_minor_version_when_auto_upgrade_enabled() {
        Config config = getConfig();
        config.setProperty(PROP_AUTO_UPGRADE_ENABLED, "true");

        upgradeClusterMembers(factory, clusterMembers, MEMBER_VERSION_NEXT_MINOR, config);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertClusterVersion(clusterMembers, CLUSTER_VERSION_NEXT_MINOR);
            }
        });
    }

    @Test
    public void cluster_version_is_upgraded_when_members_upgraded_in_parallel() {
        Config config = getConfig();
        config.setProperty(PROP_AUTO_UPGRADE_ENABLED, "true");

        clusterMembers = upgradeClusterMembersParallel(factory, clusterMembers, MEMBER_VERSION_NEXT_MINOR, config);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertClusterVersion(clusterMembers, CLUSTER_VERSION_NEXT_MINOR);
            }
        }, 20);
    }


    public HazelcastInstance[] upgradeClusterMembersParallel(final TestHazelcastInstanceFactory factory,
                                                             final HazelcastInstance[] membersToUpgrade,
                                                             final MemberVersion version,
                                                             final Config config) {
        final ConcurrentLinkedQueue<HazelcastInstance> upgradedInstances = new ConcurrentLinkedQueue<HazelcastInstance>();

        try {
            ExecutorService executorService = Executors.newFixedThreadPool(membersToUpgrade.length);

            Collection<Callable<Object>> tasks = new ArrayList<Callable<Object>>();

            overrideMemberVersionRule.setOrClearProperty(version.toString());

            for (int i = 0; i < membersToUpgrade.length; i++) {
                final int index = i;

                tasks.add(new Callable<Object>() {
                    @Override
                    public Object call() {
                        membersToUpgrade[index].shutdown();
                        upgradedInstances.add(factory.newHazelcastInstance(config));
                        return upgradedInstances.size();
                    }
                });
            }

            executorService.invokeAll(tasks);
            executorService.shutdown();
            executorService.awaitTermination(120, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return new ArrayList<HazelcastInstance>(upgradedInstances).toArray(new HazelcastInstance[membersToUpgrade.length]);
    }
}
