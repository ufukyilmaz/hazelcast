package com.hazelcast.map.hotrestart;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.hotrestart.HotRestartTestSupport;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.PER_PARTITION;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class MapHotRestartStressTest extends HotRestartTestSupport {

    private static final int INSTANCE_COUNT = 4;
    private static final int THREAD_COUNT = 4;

    @Parameters(name = "memoryFormat:{0} fsync:{2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, 1000, false},
                {InMemoryFormat.NATIVE, 1000, false},
        });
    }

    @Parameter
    public InMemoryFormat memoryFormat;

    @Parameter(1)
    public int keyRange;

    @Parameter(2)
    public boolean fsyncEnabled;

    private ConcurrentMap<Integer, Integer> localMap;
    private IMap<Integer, Integer> map;
    private String name;

    private volatile boolean running = true;

    @Before
    public void setupInternal() {
        localMap = new ConcurrentHashMap<Integer, Integer>();
        name = randomString();
    }

    @Test(timeout = 10 * 60 * 1000)
    public void test() throws Exception {
        resetFixture();
        ArrayList<Future> futures = new ArrayList<Future>();
        for (int i = 0; i < THREAD_COUNT; i++) {
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
            Integer value = map.get(i);
            if (value != null) {
                localMap.put(i, value);
            }
        }
        assertEquals(localMap.size(), map.size());

        resetFixture();
        for (int i = 0; i < keyRange; i++) {
            Integer localValue = localMap.get(i);
            Integer value = map.get(i);

            if (localValue == null) {
                assertNull("Value appeared after restart!!!", value);
            } else {
                assertEquals("Value lost/changed after restart", localValue, value);
            }
        }
    }

    private Config makeConfig() {
        Config config = new XmlConfigBuilder().build()
                .setLicenseKey(SampleLicense.UNLIMITED_LICENSE)
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "20");

        config.getNativeMemoryConfig()
                .setEnabled(true)
                .setSize(new MemorySize(256, MemoryUnit.MEGABYTES))
                .setMetadataSpacePercentage(50);

        config.getHotRestartPersistenceConfig()
                .setEnabled(true)
                .setBaseDir(baseDir);

        MaxSizeConfig maxSizeConfig = new MaxSizeConfig()
                .setMaxSizePolicy(PER_PARTITION)
                .setSize(50);

        config.getMapConfig("default").getHotRestartConfig()
                .setEnabled(true);

        MapConfig mapConfig = config.getMapConfig("native*")
                .setInMemoryFormat(NATIVE)
                .setEvictionPolicy(LFU)
                .setMaxSizeConfig(maxSizeConfig);
        mapConfig.getHotRestartConfig()
                .setEnabled(true)
                .setFsync(fsyncEnabled);

        return config;
    }

    private void resetFixture() {
        restartCluster(INSTANCE_COUNT, this::makeConfig);
        if (memoryFormat == NATIVE) {
            map = getFirstInstance().getMap("native-" + name);
        } else {
            map = getFirstInstance().getMap(name);
        }
    }
}
