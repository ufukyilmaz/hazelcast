package com.hazelcast.internal.cluster.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.cluster.ClusterVersionListener;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterpriseClusterVersionTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private HazelcastInstance instance;
    private ClusterServiceImpl cluster;
    private CountDownLatch clusterVersionUpgradeLatch;

    @Before
    public void setup() {
        instance = createHazelcastInstance();
        cluster = (ClusterServiceImpl) instance.getCluster();
        // expecting countdown twice: once upon listener registration, once more on version upgrade
        clusterVersionUpgradeLatch = new CountDownLatch(2);
        getNode(instance).getNodeExtension().registerListener(new ClusterVersionChangedListener(clusterVersionUpgradeLatch));
    }

    @Test
    public void test_clusterVersionUpgradeFails_whenNodeMajorVersionPlusOne() {
        Version version = cluster.getClusterVersion();
        assertEquals(Version.of(BuildInfoProvider.getBuildInfo().getVersion()), version);
        Version newVersion = Version.of(version.getMajor() + 1, version.getMinor());

        expectedException.expect(VersionMismatchException.class);
        cluster.changeClusterVersion(newVersion);
    }

    @Test
    public void test_clusterVersionUpgradeSucceeds_whenNodeMinorVersionPlusOne() {
        // since Node.version is private final, manipulate cluster version to current minor-1
        Version originalVersion = cluster.getClusterVersion();
        assumeTrue(originalVersion.getMinor() > 0);
        // directly set cluster version to {major, minor-1, patch}
        Version olderVersion = Version.of(originalVersion.getMajor(), originalVersion.getMinor() - 1);
        // supplant with older version
        cluster.getClusterStateManager().clusterVersion = olderVersion;

        cluster.changeClusterVersion(originalVersion);
        assertOpenEventually(clusterVersionUpgradeLatch);
    }

    public static class ClusterVersionChangedListener implements ClusterVersionListener {
        private final CountDownLatch latch;

        public ClusterVersionChangedListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onClusterVersionChange(Version newVersion) {
            latch.countDown();
        }
    }
}
