package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.internal.partition.IPartition.MAX_BACKUP_COUNT;
import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.getKnownPreviousVersionsCount;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category(CompatibilityTest.class)
public class BackupExpirationCompatibilityTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "BackupExpirationCompatibilityTest";
    // all known previous releases + current
    private static final int NODE_COUNT = getKnownPreviousVersionsCount() + 1;
    private static final int BACKUP_COUNT = min(NODE_COUNT - 1, MAX_BACKUP_COUNT);
    private static final int REPLICA_COUNT = BACKUP_COUNT + 1;
    private static final int ENTRY_COUNT = 100;

    private HazelcastInstance[] instances;

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameters(name = "inMemoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {BINARY},
                {OBJECT},
                {NATIVE},
        });
    }

    @Before
    public void setUp() {
        Config config = newConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);
        instances = factory.newInstances(config);
    }

    @Test
    public void all_backups_should_be_empty_eventually() {
        assertClusterSizeEventually(NODE_COUNT, instances);

        IMap<Integer, Integer> map = instances[0].getMap(MAP_NAME);
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(i, i);
        }

        assertEquals(ENTRY_COUNT * REPLICA_COUNT, totalEntryCountOnNodes(instances));

        try {
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() {
                    assertEquals(0, totalEntryCountOnNodes(instances));
                }
            }, 240);
        } catch (AssertionError e) {
            dumpEntryCounts();
            throw e;
        }
    }

    private Config newConfig() {
        Config config = getConfig();
        if (inMemoryFormat == NATIVE) {
            config = getHDConfig();
        }

        config.getMapConfig(MAP_NAME)
                .setBackupCount(BACKUP_COUNT)
                .setMaxIdleSeconds(3)
                .setInMemoryFormat(inMemoryFormat);

        return config;
    }

    private void dumpEntryCounts() {
        for (HazelcastInstance instance : instances) {
            IMap map = instance.getMap(MAP_NAME);
            LocalMapStats stats = map.getLocalMapStats();
            long ownedEntryCount = stats.getOwnedEntryCount();
            long backupEntryCount = stats.getBackupEntryCount();
            System.out.println(format("Instance %s, owned entries: %d, backup entries: %d", instance.getCluster().getLocalMember(),
                    ownedEntryCount, backupEntryCount));
        }
    }

    private static long totalEntryCountOnNodes(HazelcastInstance[] instances) {
        long sum = 0;
        for (HazelcastInstance instance : instances) {
            sum += getTotalEntryCountOnNode(instance);
        }
        return sum;
    }

    private static long getTotalEntryCountOnNode(HazelcastInstance instance) {
        IMap map = instance.getMap(MAP_NAME);
        LocalMapStats localMapStats = map.getLocalMapStats();
        long ownedEntryCount = localMapStats.getOwnedEntryCount();
        long backupEntryCount = localMapStats.getBackupEntryCount();
        return ownedEntryCount + backupEntryCount;
    }
}
