package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.getKnownPreviousVersionsCount;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category(CompatibilityTest.class)
public class BackupExpirationCompatibilityTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "BackupExpirationCompatibilityTest";
    private static final int NODE_COUNT = getKnownPreviousVersionsCount() + 1; // all known 3.8.X releases + 3.9
    private static final int BACKUP_COUNT = NODE_COUNT - 1;
    private static final int REPLICA_COUNT = BACKUP_COUNT + 1;
    private static final int ENTRY_COUNT = 100;

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance[] instances;

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameters(name = "inMemoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {BINARY}, {OBJECT}, {NATIVE}
        });
    }

    @Before
    public void setUp() throws Exception {
        Config config = newConfig();
        factory = createHazelcastInstanceFactory(NODE_COUNT);
        instances = factory.newInstances(config);
    }

    @Test
    public void all_backups_should_be_empty_eventually() {
        assertClusterSizeEventually(NODE_COUNT, instances);

        IMap map = instances[0].getMap(MAP_NAME);
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(i, i);
        }

        assertEquals(ENTRY_COUNT * REPLICA_COUNT, totalEntryCountOnNodes(instances));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, totalEntryCountOnNodes(instances));
            }
        }, 240);
    }

    public static long totalEntryCountOnNodes(HazelcastInstance[] instances) {
        long sum = 0;
        for (HazelcastInstance instance : instances) {
            sum += getTotalEntryCountOnNode(instance);
        }
        return sum;
    }

    public static long getTotalEntryCountOnNode(HazelcastInstance instance) {
        IMap map = instance.getMap(MAP_NAME);
        LocalMapStats localMapStats = map.getLocalMapStats();
        long ownedEntryCount = localMapStats.getOwnedEntryCount();
        long backupEntryCount = localMapStats.getBackupEntryCount();
        return ownedEntryCount + backupEntryCount;
    }

    protected Config newConfig() {
        Config config = getConfig();
        if (inMemoryFormat == NATIVE) {
            config = getHDConfig();
        }

        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        mapConfig.setBackupCount(BACKUP_COUNT);
        mapConfig.setMaxIdleSeconds(3);
        mapConfig.setInMemoryFormat(inMemoryFormat);

        return config;
    }
}