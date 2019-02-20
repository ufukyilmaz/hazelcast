package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.hotrestart.HotRestartFolderRule;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;

import static com.hazelcast.cp.internal.MetadataRaftGroupManager.INITIAL_METADATA_GROUP_ID;
import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HotRestartCPSubsystemTest extends HazelcastRaftTestSupport {

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule(true);

    private File baseDir;

    @Before
    public void before() {
        baseDir = hotRestartFolderRule.getBaseDir();
    }

    /**
     * @see com.hazelcast.cp.internal.CPMemberAddRemoveTest#testCPMemberIdentityChanges_whenLocalMemberIsRecovered_duringRestart()
     */
    @Test
    public void testCPMemberIdentityChanges_whenLocalMemberIsRecovered_withHotRestart() throws Exception {
        int cpMemberCount = 3;
        final HazelcastInstance[] instances = new HazelcastInstance[cpMemberCount];

        for (int i = 0; i < instances.length; i++) {
            Address address = factory.nextAddress();
            instances[i] = factory.newHazelcastInstance(address, newConfig(cpMemberCount));
        }

        waitUntilCPDiscoveryCompleted(instances);
        waitAllForLeaderElection(instances, INITIAL_METADATA_GROUP_ID);

        warmUpPartitions(instances);
        changeClusterStateEventually(instances[0], ClusterState.FROZEN);

        Member localMember = instances[0].getCluster().getLocalMember();
        CPMember localCpMember = instances[0].getCPSubsystem().getLocalCPMember();

        instances[0].getLifecycleService().terminate();
        assertClusterSizeEventually(2, instances[1], instances[2]);

        instances[1].getCPSubsystem().getCPSubsystemManagementService().removeCPMember(localCpMember.getUuid()).get();

        instances[0] = factory.newHazelcastInstance(localMember.getAddress(), newConfig(cpMemberCount));

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertNull(instances[0].getCPSubsystem().getLocalCPMember());
            }
        }, 5);

        instances[0].getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotNull(instances[0].getCPSubsystem().getLocalCPMember());
            }
        });
        assertNotEquals(localCpMember, getRaftService(instances[0]).getLocalCPMember());
    }

    private Config newConfig(int cpMemberCount) {
        Config config = createConfig(cpMemberCount, cpMemberCount)
                .setLicenseKey(SampleLicense.UNLIMITED_LICENSE);

        config.getHotRestartPersistenceConfig().setEnabled(true)
                .setBaseDir(baseDir)
                .setValidationTimeoutSeconds(10).setDataLoadTimeoutSeconds(10);

        return config;
    }
}
