package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.cluster.ClusterVersionListener;
import com.hazelcast.internal.cluster.impl.VersionMismatchException;
import com.hazelcast.version.Version;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.version.MemberVersion;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.test.TestClusterUpgradeUtils.assertClusterVersion;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

/**
 * Create a cluster, then change cluster version. This test uses artificial version numbers, to avoid dependence
 * on any particular version.
 */
public abstract class AbstractClusterUpgradeTest extends HazelcastTestSupport {

    static final Version CLUSTER_VERSION_2_0 = Version.of("2.0");
    static final Version CLUSTER_VERSION_2_1 = Version.of("2.1");
    static final Version CLUSTER_VERSION_2_2 = Version.of("2.2");
    static final Version CLUSTER_VERSION_2_3 = Version.of("2.3");

    static final MemberVersion VERSION_2_0_5 = MemberVersion.of(2, 0, 5);
    static final MemberVersion VERSION_2_1_0 = MemberVersion.of(2, 1, 0);
    static final MemberVersion VERSION_2_1_1 = MemberVersion.of(2, 1, 1);
    static final MemberVersion VERSION_2_2_0 = MemberVersion.of(2, 2, 0);
    static final MemberVersion VERSION_2_3_0 = MemberVersion.of(2, 3, 0);
    static final MemberVersion VERSION_3_0_0 = MemberVersion.of(3, 0, 0);

    static final int CLUSTER_MEMBERS_COUNT = 3;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    protected HazelcastInstance[] clusterMembers;

    @Before
    public final void setupCluster() {
        // Initialize a cluster at version 2.1.0
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, VERSION_2_1_0.toString());
        clusterMembers = new HazelcastInstance[CLUSTER_MEMBERS_COUNT];
        for (int i=0; i < CLUSTER_MEMBERS_COUNT; i++) {
            clusterMembers[i] = createHazelcastInstance(VERSION_2_1_0, getConfig());
        }
    }

    @Test
    public void test_upgradePatchCodebaseVersion_isAllowed() {
        upgradeInstances(clusterMembers, VERSION_2_1_1, getConfig());
        assertClusterVersion(clusterMembers, CLUSTER_VERSION_2_1);
        assertNodesVersion(VERSION_2_1_1);
    }

    @Test
    public void test_upgradeMinorVersion_isAllowed() {
        upgradeInstances(clusterMembers, VERSION_2_2_0, getConfig());
        getClusterService(clusterMembers[0]).changeClusterVersion(CLUSTER_VERSION_2_2);
        assertClusterVersion(clusterMembers, CLUSTER_VERSION_2_2);
        assertNodesVersion(VERSION_2_2_0);
    }

    // according to spec one should be able to only upgrade one minor version at a time
    // in the implementation, this condition is relaxed on purpose
    @Test
    public void test_upgradeTwoMinorVersions_isAllowed() {
        upgradeInstances(clusterMembers, VERSION_2_3_0, getConfig());
        getClusterService(clusterMembers[0]).changeClusterVersion(CLUSTER_VERSION_2_3);
        assertClusterVersion(clusterMembers, CLUSTER_VERSION_2_3);
        assertNodesVersion(VERSION_2_3_0);
    }

    @Test
    public void test_downgradeClusterVersionAllowed_afterMinorVersionUpgrade() {
        upgradeInstances(clusterMembers, VERSION_2_2_0, getConfig());
        getClusterService(clusterMembers[0]).changeClusterVersion(CLUSTER_VERSION_2_2);
        assertClusterVersion(clusterMembers, CLUSTER_VERSION_2_2);
        assertNodesVersion(VERSION_2_2_0);
        // revert cluster version to previous, to allow for 2.1.0 members to join
        getClusterService(clusterMembers[0]).changeClusterVersion(CLUSTER_VERSION_2_1);
        assertClusterVersion(clusterMembers, CLUSTER_VERSION_2_1);
        upgradeInstances(clusterMembers, VERSION_2_1_0, getConfig());
        assertNodesVersion(VERSION_2_1_0);
    }

    @Test
    public void test_upgradeMajorVersion_notAllowed() {
        expectedException.expect(IllegalStateException.class);
        upgradeInstances(clusterMembers, VERSION_3_0_0, getConfig());
    }

    @Test
    public void test_addNodeOfLesserThanClusterVersion_notAllowed() {
        expectedException.expect(IllegalStateException.class);
        createHazelcastInstance(VERSION_2_0_5, getConfig());
    }

    @Test
    public void test_decreaseClusterVersion_allowedForCompatibleMinorVersions() {
        getClusterService(clusterMembers[0]).changeClusterVersion(CLUSTER_VERSION_2_0);
        assertClusterVersion(clusterMembers, CLUSTER_VERSION_2_0);
    }

    @Test
    public void test_decreaseClusterVersion_disallowedForIncompatibleMinorVersions() {
        upgradeInstances(clusterMembers, VERSION_2_2_0, getConfig());
        getClusterService(clusterMembers[0]).changeClusterVersion(CLUSTER_VERSION_2_2);
        assertClusterVersion(clusterMembers, CLUSTER_VERSION_2_2);
        assertNodesVersion(VERSION_2_2_0);
        expectedException.expect(VersionMismatchException.class);
        getClusterService(clusterMembers[0]).changeClusterVersion(CLUSTER_VERSION_2_0);
    }

    // when a member of version A attempts to join cluster at the same time as
    // cluster version being upgraded to version B (where A is incompatible to B),
    // member fails to join.
    @Test
    public void test_memberIncompatibleToNewClusterVersionDoesNotJoin_duringClusterVersionChange()
            throws InterruptedException {
        // upgrade cluster from 2.1.0 to 2.2.0
        upgradeInstances(clusterMembers, VERSION_2_2_0, getConfig());

        // register a sleepy listener that will make new cluster version commit phase last at least 5 seconds
        getNode(clusterMembers[0]).getNodeExtension().registerListener(new ClusterVersionListener() {
            private int count = 0;

            @Override
            public void onClusterVersionChange(Version newVersion) {
                count++;
                if (count == 2) {
                    // first time invoked is on listener registration, sleep a lot during
                    // cluster version change
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        final AtomicBoolean failed = new AtomicBoolean(false);
        final StringBuilder errors = new StringBuilder();
        final CountDownLatch startSyncLatch = new CountDownLatch(1);
        final CountDownLatch finished = new CountDownLatch(2);

        new Thread() {
            @Override
            public void run() {
                try {
                    startSyncLatch.await();
                    // cluster version change was just requested
                    // ensure the cluster version listener is sleeping (commit phase)
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                try {
                    HazelcastInstance joiner = createHazelcastInstance(VERSION_2_1_0, getConfig());
                }
                catch (Exception e) {
                    if (e instanceof IllegalStateException && e.getMessage().contains("Node failed to start")) {
                        // success
                        finished.countDown();
                    } else {
                        errors.append("Joiner startup failed with " + e.getClass() + ": " + e.getMessage() + "\\n");
                        failed.set(true);
                    }
                }
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                try {
                    startSyncLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {
                    getClusterService(clusterMembers[0]).changeClusterVersion(VERSION_2_2_0.asVersion());
                    finished.countDown();
                }
                catch (Exception e) {
                    errors.append("Cluster version upgrade failed with " + e.getClass() + ": " + e.getMessage() + "\\n");
                    failed.set(true);
                }
            }
        }.start();

        startSyncLatch.countDown();
        finished.await(30, SECONDS);
        if (failed.get()) {
            System.err.println(errors);
        } else {
            assertClusterVersion(clusterMembers, CLUSTER_VERSION_2_2);
            assertClusterSize(CLUSTER_MEMBERS_COUNT, clusterMembers[0]);
        }
    }

    abstract HazelcastInstance createHazelcastInstance(MemberVersion version, Config config);
    abstract void upgradeInstances(HazelcastInstance[] instances, MemberVersion version, Config config);

    void assertNodesVersion(MemberVersion version) {
        for (int i=0; i < CLUSTER_MEMBERS_COUNT; i++) {
            assertEquals(version, getNode(clusterMembers[i]).getVersion());
        }
    }
}
