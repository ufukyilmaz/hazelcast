package com.hazelcast.map.hotrestart;

import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.internal.properties.GroupProperty;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.PER_PARTITION;
import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.nio.IOUtil.toFileName;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class MapHotRestartStressTest extends HazelcastTestSupport {

    static final int instance_count = 4;
    static final int threadCount = 4;
    @Rule
    public TestName testName = new TestName();
    @Parameterized.Parameter(0)
    public InMemoryFormat memoryFormat;

    @Parameterized.Parameter(1)
    public int keyRange;
    ConcurrentMap<Integer, Integer> localMap;
    TestHazelcastInstanceFactory fac;
    File homeDir;
    IMap map;
    String name;
    volatile boolean running = true;

    @Parameterized.Parameters(name = "memoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {InMemoryFormat.NATIVE, 1000},
                {InMemoryFormat.BINARY, 1000},
        });
    }

    @Before
    public void deleteHomeDir() {
        homeDir = new File(toFileName(getClass().getSimpleName()) + '_' + toFileName(testName.getMethodName()));
        delete(homeDir);
        if (!homeDir.mkdir() && !homeDir.exists()) {
            throw new AssertionError("Unable to create test folder: " + homeDir.getAbsolutePath());
        }
        localMap = new ConcurrentHashMap<Integer, Integer>();
        name = randomString();
    }

    @After
    public void tearDown() {
        if (fac != null) {
            fac.terminateAll();
        }
    }

    @Test(timeout = 10 * 60 * 1000)
    public void test() throws Exception {
        resetFixture(instance_count);
        ArrayList<Future> futures = new ArrayList<Future>();
        for (int i = 0; i < threadCount; i++) {
            Future future = spawn(new Runnable() {
                @Override
                public void run() {
                    Random random = new Random();
                    while (running) {
                        int key = random.nextInt(keyRange);
                        int val = random.nextInt();
                        int rand = random.nextInt(3);
                        if (rand == 0) {
                            map.remove(key);
                        } else {
                            map.put(key, val);
                        }
                    }
                }
            });
            futures.add(future);
        }
        sleepSeconds(5 * 60);
        running = false;
        waitWithDeadline(futures, 10, TimeUnit.SECONDS);
        sleepSeconds(10);
        for (int i = 0; i < keyRange; i++) {
            Integer value = (Integer) map.get(i);
            if (value != null) {
                localMap.put(i, value);
            }
        }
        assertEquals(localMap.size(), map.size());

        resetFixture(instance_count);
        for (int i = 0; i < keyRange; i++) {
            Integer localValue = localMap.get(i);
            Integer value = (Integer) map.get(i);

            if (localValue == null) {
                assertNull("Value appeared after restart!!!", value);
            } else {
                assertEquals("Value lost/changed after restart", localValue, value);
            }
        }
    }

    private Config makeConfig(int instanceId) {
        Config config = new XmlConfigBuilder()
                .build()
                .setInstanceName("hr-test-" + instanceId);
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), SampleLicense.UNLIMITED_LICENSE);
        HotRestartPersistenceConfig hrConfig = config.getHotRestartPersistenceConfig();
        hrConfig.setBaseDir(homeDir).setEnabled(true);
        config.getNativeMemoryConfig().setEnabled(true).setSize(new MemorySize(256, MemoryUnit.MEGABYTES))
                .setMetadataSpacePercentage(50);
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "20");
        config.getMapConfig("default").getHotRestartConfig().setEnabled(true);
        MapConfig mapConfig = config.getMapConfig("native*");
        mapConfig.setInMemoryFormat(NATIVE);
        mapConfig.getHotRestartConfig().setEnabled(true);
        mapConfig.setEvictionPolicy(LFU);
        mapConfig.setEvictionPercentage(10);
        mapConfig.setMinEvictionCheckMillis(0);
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig();
        maxSizeConfig.setMaxSizePolicy(PER_PARTITION).setSize(50);
        mapConfig.setMaxSizeConfig(maxSizeConfig);
        return config;
    }

    private void resetFixture(int clusterSize) throws Exception {
        if (fac != null) {
            fac.terminateAll();
            sleepAtLeastSeconds(2);
        }

        String[] addresses = new String[clusterSize];
        Arrays.fill(addresses, "127.0.0.1");
        fac = new TestHazelcastInstanceFactory(5000, addresses);

        final CountDownLatch latch = new CountDownLatch(clusterSize);
        for (int i = 0; i < clusterSize; i++) {
            final Address address = new Address("127.0.0.1", 5000 + i);
            final Config config = makeConfig(i);
            new Thread() {
                @Override
                public void run() {
                    fac.newHazelcastInstance(address, config);
                    latch.countDown();
                }
            }.start();
        }

        assertOpenEventually(latch);

        HazelcastInstance hz = fac.getAllHazelcastInstances().iterator().next();
        if (memoryFormat == NATIVE) {
            map = hz.getMap("native-" + name);
        } else {
            map = hz.getMap(name);
        }
    }
}
