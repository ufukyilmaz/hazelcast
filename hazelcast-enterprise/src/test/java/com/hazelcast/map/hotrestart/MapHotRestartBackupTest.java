package com.hazelcast.map.hotrestart;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapHotRestartBackupTest extends AbstractMapHotRestartTest {

    private static final int KEY_COUNT = 1000;

    private IMap<Integer, String> map;
    private int clusterSize;
    private int backupCount;
    private IMap[] maps;

    @Parameterized.Parameters(name = "memoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {InMemoryFormat.BINARY, KEY_COUNT, false},
                {InMemoryFormat.NATIVE, KEY_COUNT, false},
        });
    }

    @Override
    void setupInternal() {
        clusterSize = 3;
        backupCount = clusterSize - 1;
        maps = new IMap[clusterSize];
    }

    @Test
    public void test_whenClusterIsStable() throws Exception {
        HazelcastInstance[] instances = newInstances(clusterSize, backupCount);
        warmUpPartitions(instances);

        int k = 0;
        for (HazelcastInstance instance : instances) {
            maps[k++] = createMap(instance);
        }
        map = maps[maps.length - 1];

        Random random = new Random();
        for (int i = 0; i < 1; i++) {
            fillMapAndRemoveRandom(random);
        }

        waitAllForSafeState(instances);

        assertExpectedTotalMapSize(maps);
    }

    @Test
    public void test_afterMigration() throws Exception {
        HazelcastInstance hz = newHazelcastInstance(backupCount);
        map = createMap(hz);

        Random random = new Random();
        for (int i = 0; i < 1; i++) {
            fillMapAndRemoveRandom(random);
        }

        IMap[] maps = new IMap[clusterSize];
        maps[0] = map;
        for (int i = 1; i < clusterSize; i++) {
            HazelcastInstance instance = newHazelcastInstance(backupCount);
            maps[i] = createMap(instance);
        }

        assertExpectedTotalMapSize(maps);
    }

    @Test
    public void test_afterRestart() throws Exception {
        HazelcastInstance[] instances = newInstances(clusterSize, backupCount);
        warmUpPartitions(instances);

        for (int i = 0; i < instances.length; i++) {
            maps[i] = createMap(instances[i]);
        }
        map = maps[maps.length - 1];

        Random random = new Random();
        for (int i = 0; i < 1; i++) {
            fillMapAndRemoveRandom(random);
        }

        waitAllForSafeState(instances);

        instances = restartInstances(clusterSize, backupCount);
        for (int i = 0; i < instances.length; i++) {
            maps[i] = createMap(instances[i]);
        }
        map = maps[maps.length - 1];
        assertExpectedTotalMapSize(maps);
    }

    private void assertExpectedTotalMapSize(final IMap[] maps) {
        final int expectedSize = map.size() * clusterSize;

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int actualSize = 0;
                for (IMap m : maps) {
                    LocalMapStats localMapStats = m.getLocalMapStats();
                    long totalEntryCount = localMapStats.getOwnedEntryCount() + localMapStats.getBackupEntryCount();
                    actualSize += totalEntryCount;
                }
                assertEquals(expectedSize, actualSize);
            }
        });
    }

    private void fillMapAndRemoveRandom(Random random) {
        for (int key = 0; key < KEY_COUNT; key++) {
            String value = randomString();
            map.put(key, value);
        }
        map.remove(0);
        for (int i = 0; i < KEY_COUNT / 10; i++) {
            final int key = random.nextInt(KEY_COUNT);
            map.remove(key);
        }
        map.put(0, randomString());
    }
}
