package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.hotrestart.HotRestartFolderRule;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.internal.cluster.impl.ClusterJoinManager.STALE_JOIN_PREVENTION_DURATION_PROP;
import static com.hazelcast.test.OverridePropertyRule.clear;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HotRestartAutoForceStartTest extends HazelcastTestSupport {

    @Rule
    public final OverridePropertyRule ruleStaleJoinPreventionDuration = clear(STALE_JOIN_PREVENTION_DURATION_PROP);

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule(true);

    private File baseDir;
    private TestHazelcastInstanceFactory factory;

    @Before
    public void before() {
        baseDir = hotRestartFolderRule.getBaseDir();
        factory = createHazelcastInstanceFactory();
    }

    @Test
    public void whenJoiningActiveCluster_thenForceStart() {
        ruleStaleJoinPreventionDuration.setOrClearProperty("5");

        HazelcastInstance[] instances = new HazelcastInstance[4];
        Address[] addresses = new Address[instances.length];

        // start cluster
        for (int i = 0; i < instances.length; i++) {
            Address address = factory.nextAddress();
            addresses[i] = address;
            instances[i] = factory.newHazelcastInstance(address, newConfig());
        }

        assertClusterSizeEventually(instances);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        instances[1].shutdown();
        instances[1] = factory.newHazelcastInstance(addresses[1], newConfig());

        assertClusterSizeEventually(instances);
    }

    @Test
    public void whenFormerMasterJoiningActiveCluster_thenForceStart() {
        ruleStaleJoinPreventionDuration.setOrClearProperty("5");

        HazelcastInstance[] instances = new HazelcastInstance[4];
        Address[] addresses = new Address[instances.length];

        // start cluster
        for (int i = 0; i < instances.length; i++) {
            Address address = factory.nextAddress();
            addresses[i] = address;
            instances[i] = factory.newHazelcastInstance(address, newConfig());
        }

        assertClusterSizeEventually(instances);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        instances[0].shutdown();
        instances[0] = factory.newHazelcastInstance(addresses[0], newConfig());

        assertClusterSizeEventually(instances);
    }

    @Test
    public void whenMultipleMembersJoiningActiveCluster_thenForceStart() {
        ruleStaleJoinPreventionDuration.setOrClearProperty("5");

        HazelcastInstance[] instances = new HazelcastInstance[4];
        Address[] addresses = new Address[instances.length];

        // start cluster
        for (int i = 0; i < instances.length; i++) {
            Address address = factory.nextAddress();
            addresses[i] = address;
            instances[i] = factory.newHazelcastInstance(address, newConfig());
        }

        assertClusterSizeEventually(instances);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        instances[0].shutdown();
        instances[1].shutdown();
        instances[0] = factory.newHazelcastInstance(addresses[0], newConfig());
        instances[1] = factory.newHazelcastInstance(addresses[1], newConfig());

        assertClusterSizeEventually(instances);
    }

    @Test
    public void whenJoiningActiveCluster_afterShutdownWhilePassive_thenForceStart() {
        HazelcastInstance[] instances = new HazelcastInstance[4];
        Address[] addresses = new Address[instances.length];

        // start cluster
        for (int i = 0; i < instances.length; i++) {
            Address address = factory.nextAddress();
            addresses[i] = address;
            instances[i] = factory.newHazelcastInstance(address, newConfig());
        }

        assertClusterSizeEventually(instances);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        changeClusterStateEventually(instances[instances.length - 1], ClusterState.PASSIVE);
        instances[0].shutdown();
        instances[1].shutdown();

        changeClusterStateEventually(instances[instances.length - 1], ClusterState.ACTIVE);
        instances[0] = factory.newHazelcastInstance(addresses[0], newConfig());
        instances[1] = factory.newHazelcastInstance(addresses[1], newConfig());

        assertClusterSizeEventually(instances);
    }

    @Test
    public void whenJoiningNotActiveCluster_afterShutdownWhileActive_thenTerminate() {
        HazelcastInstance[] instances = new HazelcastInstance[3];
        Address[] addresses = new Address[instances.length];

        // start cluster
        for (int i = 0; i < instances.length; i++) {
            Address address = factory.nextAddress();
            addresses[i] = address;
            instances[i] = factory.newHazelcastInstance(address, newConfig());
        }

        assertClusterSizeEventually(instances);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        instances[0].shutdown();

        changeClusterStateEventually(instances[1], ClusterState.FROZEN);

        try {
            instances[0] = factory.newHazelcastInstance(addresses[0], newConfig());
            fail("Should not be able to join the cluster!");
        } catch (IllegalStateException ignored) {
        }
    }

    private void assertClusterSizeEventually(HazelcastInstance... instances) {
        assertClusterSizeEventually(instances.length, instances);
    }

    private Config newConfig() {
        Config config = new Config().setLicenseKey(SampleLicense.UNLIMITED_LICENSE);

        config.getHotRestartPersistenceConfig().setEnabled(true)
                .setBaseDir(baseDir)
                .setValidationTimeoutSeconds(10).setDataLoadTimeoutSeconds(10)
                .setAutoRemoveStaleData(true);

        return config;
    }
}
