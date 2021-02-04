package com.hazelcast.map.impl.record;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.hotrestart.HotRestartFolderRule;
import com.hazelcast.internal.memory.StandardMemoryManager;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.EnterpriseMapContainer;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDIndexConfig;

/**
 * Basic map tests for HD-IMap.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDRecordTypesTest extends HazelcastTestSupport {

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule();

    @BeforeClass
    public static void setupClass() {
        System.setProperty(StandardMemoryManager.PROPERTY_DEBUG_ENABLED, "true");
    }

    @AfterClass
    public static void tearDownClass() {
        System.setProperty(StandardMemoryManager.PROPERTY_DEBUG_ENABLED, "false");
    }

    @Override
    protected Config getConfig() {
        Config config = getHDIndexConfig();
        config.getHotRestartPersistenceConfig()
                .setEnabled(true)
                .setBaseDir(hotRestartFolderRule.getBaseDir());

        MapConfig withStats = new MapConfig("with-stats").setStatisticsEnabled(true);
        withStats.setInMemoryFormat(InMemoryFormat.NATIVE);

        MapConfig onlyEviction = new MapConfig("only-eviction").setStatisticsEnabled(false);
        onlyEviction.setInMemoryFormat(InMemoryFormat.NATIVE);
        EvictionConfig evictionConfig = onlyEviction.getEvictionConfig();
        evictionConfig.setEvictionPolicy(EvictionPolicy.LFU);
        evictionConfig.setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE).setSize(80);

        MapConfig onlyValue = new MapConfig("only-value").setStatisticsEnabled(false);
        onlyValue.setInMemoryFormat(InMemoryFormat.NATIVE);
        EvictionConfig onlyValueEvictionConfig = onlyValue.getEvictionConfig();
        onlyValueEvictionConfig.setEvictionPolicy(EvictionPolicy.NONE);

        MapConfig onlyHotRestart = new MapConfig("only-hot-restart").setStatisticsEnabled(false)
                .setInMemoryFormat(InMemoryFormat.NATIVE);
        onlyHotRestart.getHotRestartConfig()
                .setEnabled(true)
                .setFsync(true);

        config.addMapConfig(withStats);
        config.addMapConfig(onlyValue);
        config.addMapConfig(onlyEviction);
        config.addMapConfig(onlyHotRestart);

        return config;
    }

    @Test//128 = (32(key)+32(value)+64(metadata)) | 112
    public void withStats() {
        test("with-stats");
    }

    @Test//80 = (32(key)+32(value)+16(metadata)) | 64
    public void withOnlyEviction() {
        test("only-eviction");
    }

    @Test//80 = (32(key)+32(value)+16(metadata)) | 64
    public void withOnlyHotRestart() {
        test("only-hot-restart");
    }

    @Test//64 = (32(key)+32(value))  | 48
    public void withOnlyValue() {
        test("only-value");
    }

    private void test(String mapName) {
        HazelcastInstance node = createHazelcastInstance(getConfig());
        IMap map = node.getMap(mapName);

        for (int i = 0; i < 100; i++) {
            map.set(i, i);
            map.set(i, i + 1);
            map.set(i, i + 2);
            map.get(i);
        }

        HiDensityStorageInfo hdStorageInfo = getHDStorageInfo(map);
        System.err.println(hdStorageInfo);
    }

    private HiDensityStorageInfo getHDStorageInfo(IMap map) {
        MapContainer container = ((MapService) ((MapProxyImpl) map).getService()).getMapServiceContext().getMapContainer(map.getName());
        EnterpriseMapContainer mapContainer = (EnterpriseMapContainer) container;
        HiDensityStorageInfo storageInfo = mapContainer.getHDStorageInfo();
        return storageInfo;
    }
}
