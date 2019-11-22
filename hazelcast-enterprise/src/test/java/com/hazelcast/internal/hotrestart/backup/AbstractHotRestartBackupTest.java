package com.hazelcast.internal.hotrestart.backup;

import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.internal.hotrestart.HotRestartIntegrationService;
import com.hazelcast.internal.hotrestart.HotRestartTestSupport;
import com.hazelcast.internal.hotrestart.InternalHotRestartService;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.properties.ClusterProperty;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static com.hazelcast.internal.hotrestart.encryption.TestHotRestartEncryptionUtils.withBasicEncryptionAtRestConfig;
import static org.junit.Assert.assertEquals;

public abstract class AbstractHotRestartBackupTest extends HotRestartTestSupport {

    private static final String MAP_NAME = "mappy";
    private static final int KEY_COUNT = 10 * 1000;

    @Parameters(name = "encrypted:{0}")
    public static Object[] data() {
        return new Object[] { false, true };
    }

    @Parameter
    public boolean encrypted;

    protected IMap<Integer, Object> map;

    private long startFromBackupSeq;
    private boolean setBackupDir;

    private Config makeConfig() {
        final MapConfig mapConfig = new MapConfig(MAP_NAME).setBackupCount(1);

        mapConfig.getHotRestartConfig().setEnabled(true);

        Config config = new Config()
                .setProperty(ClusterProperty.ENTERPRISE_LICENSE_KEY.getName(), SampleLicense.UNLIMITED_LICENSE)
                .setProperty(ClusterProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "100")
                .addMapConfig(mapConfig);
        final HotRestartPersistenceConfig persistenceConfig = config.getHotRestartPersistenceConfig();
        persistenceConfig.setEnabled(true);

        final File nodeBackupDir = new File(baseDir, "backup");

        if (startFromBackupSeq < 0) {
            persistenceConfig.setBaseDir(new File(baseDir, "original"));
        } else {
            persistenceConfig.setBaseDir(new File(nodeBackupDir, "backup-" + startFromBackupSeq));
        }
        if (setBackupDir) {
            persistenceConfig.setBackupDir(nodeBackupDir);
        }

        if (encrypted) {
            config = withBasicEncryptionAtRestConfig(config);
        }

        return config;
    }

    void resetFixture(long backupSeqToLoad, int clusterSize) {
        resetFixture(backupSeqToLoad, clusterSize, true);
    }

    void resetFixture(long backupSeqToLoad, int clusterSize, boolean setBackupDir) {
        this.startFromBackupSeq = backupSeqToLoad;
        this.setBackupDir = setBackupDir;
        restartCluster(clusterSize, new Supplier<Config>() {
            @Override
            public Config get() {
                return makeConfig();
            }
        });
        map = getFirstInstance().getMap(MAP_NAME);
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
                    final HotRestartIntegrationService hotRestartService =
                            (HotRestartIntegrationService) getNode(instance).getNodeExtension().getInternalHotRestartService();
                    if (hotRestartService.isBackupInProgress()) {
                        return false;
                    }
                }
                return true;
            }
        }, true);
    }

    static File getNodeBackupDir(HazelcastInstance instance, int backupSeq) {
        InternalHotRestartService hotRestartService = getNode(instance).getNodeExtension().getInternalHotRestartService();
        return ((HotRestartIntegrationService) hotRestartService).getBackupDir(backupSeq);
    }

    static void assertContainsAll(IMap<Integer, Object> map, Map<Integer, String> backupedMap) {
        for (int key = 0; key < backupedMap.size(); key++) {
            final String expected = backupedMap.get(key);
            assertEquals("Invalid value in map after restart", expected, map.get(key));
        }
    }

    static boolean runBackupOnNode(HazelcastInstance instance, long seq) {
        final HotRestartIntegrationService hotRestartService = (HotRestartIntegrationService)
                getNode(instance).getNodeExtension().getInternalHotRestartService();
        return hotRestartService.backup(seq);
    }
}
