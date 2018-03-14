package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.cluster.ClusterVersionListener;
import com.hazelcast.internal.cluster.impl.VersionMismatchException;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.internal.cluster.Versions.CURRENT_CLUSTER_VERSION;
import static com.hazelcast.test.TestClusterUpgradeUtils.assertClusterVersion;
import static org.junit.Assert.assertEquals;

/**
 * Creates a cluster, then change cluster version.
 * <p>
 * This test uses artificial version numbers, to avoid dependence on any particular version.
 */
@SuppressWarnings("WeakerAccess")
public abstract class AbstractClusterUpgradeTest extends HazelcastTestSupport {

    static final Version CLUSTER_VERSION_PREVIOUS_MINOR;
    static final Version CLUSTER_VERSION_CURRENT;
    static final Version CLUSTER_VERSION_NEXT_MINOR;
    static final Version CLUSTER_VERSION_2NEXT_MINOR;

    static final MemberVersion MEMBER_VERSION_PREVIOUS_MINOR;
    static final MemberVersion MEMBER_VERSION_CURRENT;
    static final MemberVersion MEMBER_VERSION_NEXT_PATCH;
    static final MemberVersion MEMBER_VERSION_NEXT_MINOR;
    static final MemberVersion MEMBER_VERSION_2NEXT_MINOR;
    static final MemberVersion MEMBER_VERSION_NEXT_MAJOR;

    static final int CLUSTER_MEMBERS_COUNT = 3;

    static {
        int currentMajor = CURRENT_CLUSTER_VERSION.getMajor();
        int currentMinor = CURRENT_CLUSTER_VERSION.getMinor();

        CLUSTER_VERSION_CURRENT = CURRENT_CLUSTER_VERSION;
        CLUSTER_VERSION_NEXT_MINOR = Version.of(currentMajor, currentMinor + 1);
        CLUSTER_VERSION_2NEXT_MINOR = Version.of(currentMajor, currentMinor + 2);
        CLUSTER_VERSION_PREVIOUS_MINOR = Version.of(currentMajor, currentMinor - 1);

        MEMBER_VERSION_CURRENT = MemberVersion.of(currentMajor, currentMinor, 0);
        MEMBER_VERSION_NEXT_PATCH = MemberVersion.of(currentMajor, currentMinor, 1);
        MEMBER_VERSION_NEXT_MINOR = MemberVersion.of(currentMajor, currentMinor + 1, 0);
        MEMBER_VERSION_2NEXT_MINOR = MemberVersion.of(currentMajor, currentMinor + 2, 0);
        MEMBER_VERSION_NEXT_MAJOR = MemberVersion.of(currentMajor + 1, 0, 0);
        MEMBER_VERSION_PREVIOUS_MINOR = MemberVersion.of(currentMajor, currentMinor - 1, 0);
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    protected HazelcastInstance[] clusterMembers;

    @Before
    public final void setupCluster() {
        // initialize a cluster at version 2.1.0
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, MEMBER_VERSION_CURRENT.toString());
        clusterMembers = new HazelcastInstance[CLUSTER_MEMBERS_COUNT];
        for (int i = 0; i < CLUSTER_MEMBERS_COUNT; i++) {
            clusterMembers[i] = createHazelcastInstance(MEMBER_VERSION_CURRENT, getConfig());
        }
    }

    @Test
    public void test_upgradePatchCodebaseVersion_isAllowed() {
        upgradeInstances(clusterMembers, MEMBER_VERSION_NEXT_PATCH, getConfig());
        assertClusterVersion(clusterMembers, CLUSTER_VERSION_CURRENT);
        assertNodesVersion(MEMBER_VERSION_NEXT_PATCH);
    }

    @Test
    public void test_upgradeMinorVersion_isAllowed() {
        upgradeInstances(clusterMembers, MEMBER_VERSION_NEXT_MINOR, getConfig());
        getClusterService(clusterMembers[0]).changeClusterVersion(CLUSTER_VERSION_NEXT_MINOR);
        assertClusterVersion(clusterMembers, CLUSTER_VERSION_NEXT_MINOR);
        assertNodesVersion(MEMBER_VERSION_NEXT_MINOR);
    }

    // according to spec one should be able to only upgrade one minor version at a time
    // in the implementation, this condition is relaxed on purpose
    @Test
    public void test_upgradeTwoMinorVersions_isAllowed() {
        upgradeInstances(clusterMembers, MEMBER_VERSION_2NEXT_MINOR, getConfig());
        getClusterService(clusterMembers[0]).changeClusterVersion(CLUSTER_VERSION_2NEXT_MINOR);
        assertClusterVersion(clusterMembers, CLUSTER_VERSION_2NEXT_MINOR);
        assertNodesVersion(MEMBER_VERSION_2NEXT_MINOR);
    }

    @Test
    public void test_downgradeClusterVersionAllowed_afterMinorVersionUpgrade() {
        upgradeInstances(clusterMembers, MEMBER_VERSION_NEXT_MINOR, getConfig());
        getClusterService(clusterMembers[0]).changeClusterVersion(CLUSTER_VERSION_NEXT_MINOR);
        assertClusterVersion(clusterMembers, CLUSTER_VERSION_NEXT_MINOR);
        assertNodesVersion(MEMBER_VERSION_NEXT_MINOR);
        // revert cluster version to previous, to allow for 2.1.0 members to join
        getClusterService(clusterMembers[0]).changeClusterVersion(CLUSTER_VERSION_CURRENT);
        assertClusterVersion(clusterMembers, CLUSTER_VERSION_CURRENT);
        upgradeInstances(clusterMembers, MEMBER_VERSION_CURRENT, getConfig());
        assertNodesVersion(MEMBER_VERSION_CURRENT);
    }

    @Test
    public void test_upgradeMajorVersion_notAllowed() {
        expectedException.expect(IllegalStateException.class);
        upgradeInstances(clusterMembers, MEMBER_VERSION_NEXT_MAJOR, getConfig());
    }

    @Test
    public void test_addNodeOfLesserThanClusterVersion_notAllowed() {
        expectedException.expect(IllegalStateException.class);
        createHazelcastInstance(MEMBER_VERSION_PREVIOUS_MINOR, getConfig());
    }

    @Test
    public void test_decreaseClusterVersion_allowedForCompatibleMinorVersions() {
        getClusterService(clusterMembers[0]).changeClusterVersion(CLUSTER_VERSION_PREVIOUS_MINOR);
        assertClusterVersion(clusterMembers, CLUSTER_VERSION_PREVIOUS_MINOR);
    }

    @Test
    public void test_decreaseClusterVersion_disallowedForIncompatibleMinorVersions() {
        upgradeInstances(clusterMembers, MEMBER_VERSION_NEXT_MINOR, getConfig());
        getClusterService(clusterMembers[0]).changeClusterVersion(CLUSTER_VERSION_NEXT_MINOR);
        assertClusterVersion(clusterMembers, CLUSTER_VERSION_NEXT_MINOR);
        assertNodesVersion(MEMBER_VERSION_NEXT_MINOR);
        expectedException.expect(VersionMismatchException.class);
        getClusterService(clusterMembers[0]).changeClusterVersion(CLUSTER_VERSION_PREVIOUS_MINOR);
    }

    // when a member of version A attempts to join cluster at the same time as
    // cluster version being upgraded to version B (where A is incompatible to B),
    // member fails to join.
    @Test
    public void test_memberIncompatibleToNewClusterVersionDoesNotJoin_duringClusterVersionChange() {
        // upgrade cluster from 2.1.0 to 2.2.0
        upgradeInstances(clusterMembers, MEMBER_VERSION_NEXT_MINOR, getConfig());

        // register a sleepy listener that will make new cluster version commit phase last at least 5 seconds
        getNode(clusterMembers[0]).getNodeExtension().registerListener(new ClusterVersionListener() {
            private int count = 0;

            @Override
            public void onClusterVersionChange(Version newVersion) {
                count++;
                if (count == 2) {
                    // first time invoked is on listener registration, sleep a lot during
                    // cluster version change
                    sleepSeconds(5);
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
                    sleepSeconds(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                try {
                    createHazelcastInstance(MEMBER_VERSION_CURRENT, getConfig());
                } catch (Exception e) {
                    if (e instanceof IllegalStateException && e.getMessage().contains("Node failed to start")) {
                        // success
                        finished.countDown();
                    } else {
                        errors.append("Joiner startup failed with " + e.getClass() + ": " + e.getMessage() + "\n");
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
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {
                    getClusterService(clusterMembers[0]).changeClusterVersion(MEMBER_VERSION_NEXT_MINOR.asVersion());
                    finished.countDown();
                } catch (Exception e) {
                    errors.append("Cluster version upgrade failed with " + e.getClass() + ": " + e.getMessage() + "\n");
                    failed.set(true);
                }
            }
        }.start();

        startSyncLatch.countDown();
        assertOpenEventually(finished, 30);
        if (failed.get()) {
            System.err.println(errors);
        } else {
            assertClusterVersion(clusterMembers, CLUSTER_VERSION_NEXT_MINOR);
            assertClusterSize(CLUSTER_MEMBERS_COUNT, clusterMembers[0]);
        }
    }

    abstract HazelcastInstance createHazelcastInstance(MemberVersion version, Config config);

    abstract void upgradeInstances(HazelcastInstance[] instances, MemberVersion version, Config config);

    void assertNodesVersion(MemberVersion version) {
        for (int i = 0; i < CLUSTER_MEMBERS_COUNT; i++) {
            assertEquals(version, getNode(clusterMembers[i]).getVersion());
        }
    }
}
