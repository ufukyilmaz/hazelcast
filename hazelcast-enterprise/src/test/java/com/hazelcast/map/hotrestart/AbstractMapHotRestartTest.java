package com.hazelcast.map.hotrestart;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runners.Parameterized;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.PER_PARTITION;
import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.nio.IOUtil.toFileName;
import static org.junit.Assert.assertNotNull;

public abstract class AbstractMapHotRestartTest extends HazelcastTestSupport {

    private static InetAddress localAddress;
    @Rule
    public TestName testName = new TestName();
    @Parameterized.Parameter(0)
    public InMemoryFormat memoryFormat;
    @Parameterized.Parameter(1)
    public int keyRange;
    @Parameterized.Parameter(2)
    public boolean evictionEnabled;
    protected String mapName;
    protected File baseDir;
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

        mapName = randomString();

        factory = createFactory();

        setupInternal();
    }

    private TestHazelcastInstanceFactory createFactory() {
        String[] addresses = new String[10];
        Arrays.fill(addresses, "127.0.0.1");
        return new TestHazelcastInstanceFactory(5000, addresses);
    }

    void setupInternal() {
    }

    @After
    public final void tearDown() {
        tearDownInternal();

        if (factory != null) {
            factory.terminateAll();
        }

        if (baseDir != null) {
            delete(baseDir);
        }
    }

    void tearDownInternal() {
    }

    HazelcastInstance newHazelcastInstance() {
        return newHazelcastInstance(1);
    }

    HazelcastInstance newHazelcastInstance(int backupCount) {
        Address address = factory.nextAddress();
        return factory.newHazelcastInstance(address, makeConfig(address, backupCount));
    }

    HazelcastInstance newHazelcastInstance(Address address, Config config) {
        return factory.newHazelcastInstance(address, config);
    }

    HazelcastInstance[] newInstances(int clusterSize) {
        return newInstances(clusterSize, 1);
    }

    HazelcastInstance[] newInstances(int clusterSize, int backupCount) {
        HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        for (int i = 0; i < clusterSize; i++) {
            HazelcastInstance instance = newHazelcastInstance(backupCount);
            instances[i] = instance;
        }
        return instances;
    }

    HazelcastInstance[] restartInstances(int clusterSize) {
        return restartInstances(clusterSize, 1);
    }

    HazelcastInstance[] restartInstances(int clusterSize, final int backupCount) {
        ClusterState state = ClusterState.ACTIVE;
        if (factory != null) {
            Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
            if (!instances.isEmpty()) {
                HazelcastInstance instance = instances.iterator().next();
                Cluster cluster = instance.getCluster();
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
                    Config config = makeConfig(address, backupCount);
                    factory.newHazelcastInstance(address, config);
                    latch.countDown();
                }
            }.start();
        }

        assertOpenEventually(latch);

        Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
        if (!instances.isEmpty()) {
            HazelcastInstance instance = instances.iterator().next();
            instance.getCluster().changeClusterState(state);
        }
        return instances.toArray(new HazelcastInstance[0]);
    }

    HazelcastInstance restartHazelcastInstance(HazelcastInstance hz, Config config) {
        Address address = getNode(hz).getThisAddress();
        hz.shutdown();
        return factory.newHazelcastInstance(address, config);
    }

    Config makeConfig(Address address, int backupCount) {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), SampleLicense.UNLIMITED_LICENSE);
        config.setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "100");

        // to reduce used native memory size
        config.setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "4");

        HotRestartPersistenceConfig hotRestartPersistenceConfig = config.getHotRestartPersistenceConfig();
        hotRestartPersistenceConfig.setEnabled(true);
        hotRestartPersistenceConfig.setBaseDir(new File(baseDir, toFileName(address.getHost() + ":" + address.getPort())));

        if (memoryFormat == InMemoryFormat.NATIVE) {
            config.getNativeMemoryConfig().setEnabled(true)
                    .setSize(getNativeMemorySize())
                    .setMetadataSpacePercentage(20);
        }

        if (memoryFormat != null) {
            MapConfig mapConfig = new MapConfig(mapName);
            mapConfig.getHotRestartConfig().setEnabled(true);
            mapConfig.setInMemoryFormat(memoryFormat);
            mapConfig.setBackupCount(backupCount);
            setEvictionConfig(mapConfig);
            config.addMapConfig(mapConfig);
        }

        return config;
    }

    MemorySize getNativeMemorySize() {
        return new MemorySize(64, MemoryUnit.MEGABYTES);
    }

    <V> IMap<Integer, V> createMap() {
        HazelcastInstance hz = factory.getAllHazelcastInstances().iterator().next();
        assertNotNull(hz);
        return createMap(hz);
    }

    <V> IMap<Integer, V> createMap(HazelcastInstance hz) {
        return hz.getMap(mapName);
    }

    private void setEvictionConfig(MapConfig mapConfig) {
        if (!evictionEnabled) {
            return;
        }
        mapConfig.setEvictionPolicy(EvictionPolicy.LFU);
        if (memoryFormat == InMemoryFormat.NATIVE) {
            mapConfig.setMaxSizeConfig(new MaxSizeConfig().setMaxSizePolicy(FREE_NATIVE_MEMORY_PERCENTAGE).setSize(80));
        } else {
            mapConfig.setMaxSizeConfig(new MaxSizeConfig().setMaxSizePolicy(PER_PARTITION).setSize(50));
        }
    }
}
