package com.hazelcast.spi.hotrestart.backup;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.instance.EnterpriseNodeExtension;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.nio.IOUtil.toFileName;
import static org.junit.Assert.assertEquals;

public abstract class AbstractHotRestartBackupTest extends HazelcastTestSupport {
    private static final String MAP_NAME = "mappy";
    private static final int KEY_COUNT = 10 * 1000;
    private static InetAddress localAddress;
    @Rule
    public TestName testName = new TestName();
    private File baseDir;
    private long startFromBackupSeq;
    private boolean setBackupDir;
    protected IMap<Integer, Object> map;
    protected TestHazelcastInstanceFactory factory;

    @BeforeClass
    public static void setupClass() throws UnknownHostException {
        localAddress = InetAddress.getLocalHost();
    }

    @Before
    public final void setup() throws UnknownHostException {
        baseDir = new File(toFileName(getClass().getSimpleName()) + '_' + toFileName(testName.getMethodName()));
        delete(baseDir);
        if (!baseDir.mkdir() && !baseDir.exists()) {
            throw new AssertionError("Unable to create test folder: " + baseDir.getAbsolutePath());
        }
        factory = createFactory();
    }

    private TestHazelcastInstanceFactory createFactory() {
        String[] addresses = new String[10];
        Arrays.fill(addresses, "127.0.0.1");
        return new TestHazelcastInstanceFactory(5000, addresses);
    }

    @After
    public final void tearDown() {
        if (factory != null) {
            factory.terminateAll();
        }

        if (baseDir != null) {
            delete(baseDir);
        }
    }

    private void restartInstances(int clusterSize) {
        ClusterState state = ClusterState.ACTIVE;
        if (factory != null) {
            final Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
            if (!instances.isEmpty()) {
                final HazelcastInstance instance = instances.iterator().next();
                final Cluster cluster = instance.getCluster();
                state = cluster.getClusterState();
                cluster.changeClusterState(ClusterState.PASSIVE);
            }
            factory.terminateAll();
        }

        factory = createFactory();

        final CountDownLatch latch = new CountDownLatch(clusterSize);
        for (int i = 0; i < clusterSize; i++) {
            final Address address = new Address("127.0.0.1", localAddress, 5000 + i);
            new Thread() {
                @Override
                public void run() {
                    factory.newHazelcastInstance(address, makeConfig(address));
                    latch.countDown();
                }
            }.start();
        }

        assertOpenEventually(latch);

        final Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
        if (!instances.isEmpty()) {
            final HazelcastInstance instance = instances.iterator().next();
            instance.getCluster().changeClusterState(state);
        }
    }

    private Config makeConfig(Address address) {
        final MapConfig mapConfig = new MapConfig(MAP_NAME).setBackupCount(1);

        mapConfig.getHotRestartConfig().setEnabled(true);

        final Config config = new Config()
                .setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), SampleLicense.UNLIMITED_LICENSE)
                .setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "100")
                .addMapConfig(mapConfig);
        final HotRestartPersistenceConfig persistenceConfig = config.getHotRestartPersistenceConfig();
        persistenceConfig.setEnabled(true);

        final File persistanceBaseDir = new File(this.baseDir, toFileName(address.getHost() + ":" + address.getPort()));
        final File nodeBackupDir = new File(persistanceBaseDir, "backup");

        if (startFromBackupSeq < 0) {
            persistenceConfig.setBaseDir(new File(persistanceBaseDir, "original"));
        } else {
            persistenceConfig.setBaseDir(new File(nodeBackupDir, "backup-" + startFromBackupSeq));
        }
        if (setBackupDir) {
            persistenceConfig.setBackupDir(nodeBackupDir);
        }

        return config;
    }


    void resetFixture(long backupSeqToLoad, int clusterSize) {
        resetFixture(backupSeqToLoad, clusterSize, true);
    }

    void resetFixture(long backupSeqToLoad, int clusterSize, boolean setBackupDir) {
        this.startFromBackupSeq = backupSeqToLoad;
        this.setBackupDir = setBackupDir;
        restartInstances(clusterSize);
        map = factory.getAllHazelcastInstances().iterator().next().getMap(MAP_NAME);
    }


    void fillMap(Map<Integer, String> expectedMap) {
        for (int i = 0; i < 3; i++) {
            for (int key = 0; key < KEY_COUNT; key++) {
                String value = randomString();
                map.put(key, value);
                if (expectedMap != null) {
                    expectedMap.put(key, value);
                }
            }
        }
    }

    void waitForBackupToFinish(final Collection<HazelcastInstance> instances) {
        assertEqualsEventually(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                for (HazelcastInstance instance : instances) {
                    final EnterpriseNodeExtension extension = (EnterpriseNodeExtension) getNode(instance).getNodeExtension();
                    if (extension.getHotRestartService().isBackupInProgress()) {
                        return false;
                    }
                }
                return true;
            }
        }, true);
    }

    static File getNodeBackupDir(HazelcastInstance instance, int backupSeq) {
        final HotRestartPersistenceConfig hrConfig = instance.getConfig().getHotRestartPersistenceConfig();
        return new File(hrConfig.getBackupDir(), "backup-" + backupSeq);
    }

    static void assertContainsAll(IMap<Integer, Object> map, Map<Integer, String> backupedMap) {
        for (int key = 0; key < backupedMap.size(); key++) {
            final String expected = backupedMap.get(key);
            assertEquals("Invalid value in map after restart", expected, map.get(key));
        }
    }
}
