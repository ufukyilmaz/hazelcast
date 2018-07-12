package com.hazelcast.spi.hotrestart.backup;

import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.hotrestart.HotRestartIntegrationService;
import com.hazelcast.spi.hotrestart.HotRestartTestSupport;
import com.hazelcast.spi.properties.GroupProperty;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.hazelcast.nio.IOUtil.toFileName;
import static org.junit.Assert.assertEquals;

public abstract class AbstractHotRestartBackupTest extends HotRestartTestSupport {

    private static final String MAP_NAME = "mappy";
    private static final int KEY_COUNT = 10 * 1000;

    protected IMap<Integer, Object> map;

    private long startFromBackupSeq;
    private boolean setBackupDir;

    private Config makeConfig(Address address) {
        final MapConfig mapConfig = new MapConfig(MAP_NAME).setBackupCount(1);

        mapConfig.getHotRestartConfig().setEnabled(true);

        final Config config = new Config()
                .setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), SampleLicense.UNLIMITED_LICENSE)
                .setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "100")
                .addMapConfig(mapConfig);
        final HotRestartPersistenceConfig persistenceConfig = config.getHotRestartPersistenceConfig();
        persistenceConfig.setEnabled(true);

        final File persistenceBaseDir = new File(this.baseDir, toFileName(address.getHost() + ":" + address.getPort()));
        final File nodeBackupDir = new File(persistenceBaseDir, "backup");

        if (startFromBackupSeq < 0) {
            persistenceConfig.setBaseDir(new File(persistenceBaseDir, "original"));
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
        restartCluster(clusterSize, new IFunction<Address, Config>() {
            @Override
            public Config apply(Address address) {
                return makeConfig(address);
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
        final HotRestartPersistenceConfig hrConfig = instance.getConfig().getHotRestartPersistenceConfig();
        return new File(hrConfig.getBackupDir(), "backup-" + backupSeq);
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
