package com.hazelcast.map.hotrestart;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.Random;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapHotRestartBackupTest extends AbstractMapHotRestartTest {

    private static final int CLUSTER_SIZE = 3;
    private static final int BACKUP_COUNT = CLUSTER_SIZE - 1;

    private Random random = new Random();

    private IMap<Integer, String>[] maps;
    private IMap<Integer, String> map;

    @Parameters(name = "memoryFormat:{0} fsync:{2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, KEY_COUNT, false, false},
                {InMemoryFormat.NATIVE, KEY_COUNT, false, false},
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void setupMapInternal() {
        maps = new IMap[CLUSTER_SIZE];
    }

    @Test
    public void test_whenClusterIsStable() {
        HazelcastInstance[] instances = newInstances(CLUSTER_SIZE, BACKUP_COUNT);
        warmUpPartitions(instances);

        int k = 0;
        for (HazelcastInstance instance : instances) {
            maps[k++] = createMap(instance);
        }
        map = maps[maps.length - 1];

        for (int i = 0; i < 1; i++) {
            fillMapAndRemoveRandom(map, random);
        }

        waitAllForSafeState(instances);

        assertExpectedTotalMapSize(maps, map.size() * CLUSTER_SIZE);
    }

    @Test
    public void test_afterMigration() {
        HazelcastInstance hz = newHazelcastInstance(BACKUP_COUNT);
        map = createMap(hz);

        for (int i = 0; i < 1; i++) {
            fillMapAndRemoveRandom(map, random);
        }

        maps[0] = map;
        for (int i = 1; i < CLUSTER_SIZE; i++) {
            HazelcastInstance instance = newHazelcastInstance(BACKUP_COUNT);
            maps[i] = createMap(instance);
        }

        assertExpectedTotalMapSize(maps, map.size() * CLUSTER_SIZE);
    }

    @Test
    public void test_afterRestart() {
        HazelcastInstance[] instances = newInstances(CLUSTER_SIZE, BACKUP_COUNT);
        warmUpPartitions(instances);

        for (int i = 0; i < instances.length; i++) {
            maps[i] = createMap(instances[i]);
        }
        map = maps[maps.length - 1];

        for (int i = 0; i < 1; i++) {
            fillMapAndRemoveRandom(map, random);
        }

        waitAllForSafeState(instances);

        instances = restartInstances(CLUSTER_SIZE, BACKUP_COUNT);
        for (int i = 0; i < instances.length; i++) {
            maps[i] = createMap(instances[i]);
        }
        map = maps[maps.length - 1];
        assertExpectedTotalMapSize(maps, map.size() * CLUSTER_SIZE);
    }

    private static void fillMapAndRemoveRandom(IMap<Integer, String> map, Random random) {
        for (int key = 0; key < KEY_COUNT; key++) {
            String value = randomString();
            map.put(key, value);
        }
        map.remove(0);
        for (int i = 0; i < KEY_COUNT / 10; i++) {
            int key = random.nextInt(KEY_COUNT);
            map.remove(key);
        }
        map.put(0, randomString());
    }

    private static void assertExpectedTotalMapSize(final IMap[] maps, final int expectedSize) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                int actualSize = 0;
                for (IMap map : maps) {
                    LocalMapStats localMapStats = map.getLocalMapStats();
                    long totalEntryCount = localMapStats.getOwnedEntryCount() + localMapStats.getBackupEntryCount();
                    actualSize += totalEntryCount;
                }
                assertEquals(expectedSize, actualSize);
            }
        });
    }
}
