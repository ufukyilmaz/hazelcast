package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.File;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.nio.IOUtil.toFileName;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HotRestartAutoForceStartTest extends HazelcastTestSupport {

    @Rule
    public TestName testName = new TestName();

    private File baseDir;
    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @Before
    public void before() {
        baseDir = new File(toFileName(getClass().getSimpleName()) + '_' + toFileName(testName.getMethodName()));
        delete(baseDir);
        if (!baseDir.mkdir()) {
            throw new IllegalStateException("Failed to create hot-restart directory!");
        }
    }

    @After
    public void after() {
        factory.terminateAll();
        delete(baseDir);
    }

    @Test
    public void whenJoiningActiveCluster_thenForceStart() {
        HazelcastInstance[] instances = new HazelcastInstance[4];
        Address[] addresses = new Address[instances.length];

        // start cluster
        for (int i = 0; i < instances.length; i++) {
            Address address = factory.nextAddress();
            addresses[i] = address;
            instances[i] = factory.newHazelcastInstance(address, newConfig(address));
        }

        assertClusterSizeEventually(instances);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        instances[0].shutdown();
        instances[1].shutdown();

        instances[0] = factory.newHazelcastInstance(addresses[0], newConfig(addresses[0]));
        instances[1] = factory.newHazelcastInstance(addresses[1], newConfig(addresses[1]));

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
            instances[i] = factory.newHazelcastInstance(address, newConfig(address));
        }

        assertClusterSizeEventually(instances);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        changeClusterStateEventually(instances[instances.length - 1], ClusterState.PASSIVE);
        instances[0].shutdown();
        instances[1].shutdown();

        changeClusterStateEventually(instances[instances.length - 1], ClusterState.ACTIVE);
        instances[0] = factory.newHazelcastInstance(addresses[0], newConfig(addresses[0]));
        instances[1] = factory.newHazelcastInstance(addresses[1], newConfig(addresses[1]));

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
            instances[i] = factory.newHazelcastInstance(address, newConfig(address));
        }

        assertClusterSizeEventually(instances);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        instances[0].shutdown();

        changeClusterStateEventually(instances[1], ClusterState.FROZEN);

        try {
            instances[0] = factory.newHazelcastInstance(addresses[0], newConfig(addresses[0]));
            fail("Should not be able to join the cluster!");
        } catch (IllegalStateException ignored) {
        }
    }

    private void assertClusterSizeEventually(HazelcastInstance... instances) {
        assertClusterSizeEventually(instances.length, instances);
    }

    private Config newConfig(Address address) {
        Config config = new Config().setLicenseKey(SampleLicense.UNLIMITED_LICENSE);

        config.getHotRestartPersistenceConfig().setEnabled(true)
                .setBaseDir(new File(baseDir, toFileName(address.getHost() + ":" + address.getPort())))
                .setClusterDataRecoveryPolicy(HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_COMPLETE)
                .setValidationTimeoutSeconds(10).setDataLoadTimeoutSeconds(10)
                .setAutoRemoveStaleData(true);

        return config;
    }
}
