package com.hazelcast.map.hotrestart;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapConfigurationHotRestartTest extends AbstractMapHotRestartTest {

    private static final InMemoryFormat NON_DEFAULT_IN_MEMORY_FORMAT = InMemoryFormat.NATIVE;
    private static final InMemoryFormat DEFAULT_IN_MEMORY_FORMAT = MapConfig.DEFAULT_IN_MEMORY_FORMAT;

    @Test
    public void givenDynamicMapConfigHasHotRestartEnabled_whenClusterRestarted_thenConfigExists() {
        String mapName = randomMapName();
        int clusterSize = 3;

        HazelcastInstance[] instances = newInstances(clusterSize);
        HazelcastInstance hz = instances[0];

        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        mapConfig.getHotRestartConfig().setEnabled(true);
        hz.getConfig()
                .addMapConfig(mapConfig);

        instances = restartInstances(clusterSize);

        for (HazelcastInstance instance : instances) {
            MapConfig dynamicMapConfig = instance.getConfig().findMapConfig(mapName);
            assertEquals(NON_DEFAULT_IN_MEMORY_FORMAT, dynamicMapConfig.getInMemoryFormat());
        }
    }

    @Test
    public void givenDynamicMapConfigDoesNotHaveHotRestartEnabled_whenClusterRestarted_thenConfigExists() {
        String mapName = randomMapName();
        int clusterSize = 3;

        HazelcastInstance[] instances = newInstances(clusterSize);
        HazelcastInstance hz = instances[0];

        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        mapConfig.getHotRestartConfig().setEnabled(false);
        hz.getConfig()
                .addMapConfig(mapConfig);

        // DynamicConfigurationAwareConfig initializes the partition table
        waitAllForSafeState(instances);

        instances = restartInstances(clusterSize);
        for (HazelcastInstance instance : instances) {
            MapConfig dynamicMapConfig = instance.getConfig().findMapConfig(mapName);
            // hot restart was no configured in the map config -> the configuration was not persistent
            // hence we should get the default format after cluster restart
            assertEquals(DEFAULT_IN_MEMORY_FORMAT, dynamicMapConfig.getInMemoryFormat());
        }
    }
}
