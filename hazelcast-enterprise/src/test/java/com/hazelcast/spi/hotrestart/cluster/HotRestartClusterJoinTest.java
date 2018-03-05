package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.nio.IOUtil.toFileName;
import static java.util.Collections.synchronizedList;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HotRestartClusterJoinTest extends HazelcastTestSupport {

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
    public void hotRestart_fails_when_members_join_unknown_master() {
        Address unknownAddress = factory.nextAddress();

        HazelcastInstance[] instances = new HazelcastInstance[3];
        Address[] addresses = new Address[instances.length];
        for (int i = 0; i < instances.length; i++) {
            Address address = factory.nextAddress();
            addresses[i] = address;
            instances[i] = factory.newHazelcastInstance(address, newConfig(address));
        }

        // start and shutdown cluster
        assertClusterSizeEventually(instances);
        shutdownCluster(instances[0]);

        // start an unknown member
        HazelcastInstance hz = factory.newHazelcastInstance(unknownAddress, newConfig(unknownAddress));

        // restart previous members
        instances = startInstances(addresses);

        // hot restart should fail
        assertThat(instances, emptyArray());
        assertClusterSize(1, hz);
    }

    @Test
    public void hotRestart_fails_when_stale_members_join_master() {
        HazelcastInstance[] instances = new HazelcastInstance[4];
        Address[] addresses = new Address[instances.length];

        // start cluster
        for (int i = 0; i < instances.length; i++) {
            Address address = factory.nextAddress();
            addresses[i] = address;
            instances[i] = factory.newHazelcastInstance(address, newConfig(address));
        }

        // shutdown cluster
        assertClusterSizeEventually(instances);
        warmUpPartitions(instances);
        shutdownCluster(instances);

        // partial start and shutdown single member
        Address masterAddress = addresses[0];
        HazelcastInstance hz = factory.newHazelcastInstance(masterAddress, newConfig(masterAddress));
        shutdownCluster(hz);

        // restart single master member
        hz = factory.newHazelcastInstance(masterAddress, newConfig(masterAddress));

        // restart remaining members
        Address[] newAddresses = new Address[addresses.length - 1];
        System.arraycopy(addresses, 1, newAddresses, 0, newAddresses.length);
        instances = startInstances(newAddresses);

        // hot restart should fail
        assertThat(instances, emptyArray());
        assertClusterSize(1, hz);
    }

    @Test
    public void hotRestart_with_unknown_member() {
        Address unknownAddress = factory.nextAddress();

        HazelcastInstance[] instances = new HazelcastInstance[3];
        Address[] addresses = new Address[instances.length];
        // start cluster
        for (int i = 0; i < instances.length; i++) {
            Address address = factory.nextAddress();
            addresses[i] = address;
            instances[i] = factory.newHazelcastInstance(address, newConfig(address));
        }

        // shutdown cluster
        assertClusterSizeEventually(instances);
        shutdownCluster(instances);

        Address[] newAddresses = Arrays.copyOf(addresses, addresses.length + 1);
        newAddresses[addresses.length] = unknownAddress;

        // restart cluster with an additional unknown member
        instances = startInstances(newAddresses);

        if (instances.length == 1) {
            // If unknown member starts up first and becomes master,
            // then hot restart should fail, only unknown member will survive.
            assertEquals(unknownAddress, getAddress(instances[0]));
        } else {
            assertThat(instances, arrayWithSize(newAddresses.length));
            Member[] members = instances[0].getCluster().getMembers().toArray(new Member[0]);
            // If unknown member starts during hot restart process,
            // its join request will be delayed until hot restart completes
            // and it will be allowed to join as the last member.
            assertEquals(unknownAddress, members[members.length - 1].getAddress());
        }
    }

    @Test
    public void hotRestart_with_stale_member() {
        HazelcastInstance[] instances = new HazelcastInstance[3];
        Address[] addresses = new Address[instances.length];

        // start cluster
        for (int i = 0; i < instances.length; i++) {
            Address address = factory.nextAddress();
            addresses[i] = address;
            instances[i] = factory.newHazelcastInstance(address, newConfig(address));
        }

        // shutdown cluster
        assertClusterSizeEventually(instances);
        shutdownCluster(instances);

        // partial start cluster excluding single member
        Address excludedAddress = addresses[1];
        instances = startInstances(new Address[]{addresses[0], addresses[2]});

        // start an additional new member
        Address newMemberAddress = factory.nextAddress();
        HazelcastInstance hz = factory.newHazelcastInstance(newMemberAddress, newConfig(newMemberAddress));

        // shutdown cluster
        assertClusterSizeEventually(instances.length + 1, instances);
        assertClusterSizeEventually(instances.length + 1, hz);
        shutdownCluster(instances);

        // restart cluster with all members
        Address[] allAddresses = Arrays.copyOf(addresses, addresses.length + 1);
        allAddresses[addresses.length] = newMemberAddress;
        instances = startInstances(allAddresses);

        if (instances.length == 1) {
            // If excluded member starts up first and becomes master,
            // then only excluded member will survive.
            assertEquals(excludedAddress, getAddress(instances[0]));
        } else {
            // otherwise excluded member will fail, others will restart successfully.
            assertThat(instances, arrayWithSize(addresses.length));
            for (HazelcastInstance instance : instances) {
                assertNotEquals(excludedAddress, getAddress(instance));
            }
        }
    }

    private void assertClusterSizeEventually(HazelcastInstance... instances) {
        assertClusterSizeEventually(instances.length, instances);
    }

    private HazelcastInstance[] startInstances(Address[] addresses) {
        final List<HazelcastInstance> instancesList = synchronizedList(new ArrayList<HazelcastInstance>());
        final CountDownLatch latch = new CountDownLatch(addresses.length);

        for (final Address address : addresses) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        HazelcastInstance instance = factory.newHazelcastInstance(address, newConfig(address));
                        instancesList.add(instance);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                }
            }).start();
        }
        assertOpenEventually(latch);
        return instancesList.toArray(new HazelcastInstance[instancesList.size()]);
    }

    private Config newConfig(Address address) {
        Config config = new Config()
                .setLicenseKey(SampleLicense.UNLIMITED_LICENSE);

        config.getHotRestartPersistenceConfig().setEnabled(true)
                .setBaseDir(new File(baseDir, toFileName(address.getHost() + ":" + address.getPort())))
                .setClusterDataRecoveryPolicy(HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_COMPLETE)
                .setValidationTimeoutSeconds(10)
                .setDataLoadTimeoutSeconds(10);

        return config;
    }

    private void shutdownCluster(HazelcastInstance... instances) {
        assertThat(instances, not(emptyArray()));
        waitAllForSafeState(instances);
        instances[0].getCluster().shutdown();
    }
}
