package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.map.impl.EnterpriseMapContainer;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class})
public class HDMapExpirationMetadataStorageTest extends HazelcastTestSupport {

    private IMap map;
    private IMap mapHasExpirableKeys;

    @Before
    public void setUp() {
        Config config = getHDConfig();

        HazelcastInstance node = createHazelcastInstance(config);
        map = node.getMap("default");
        mapHasExpirableKeys = node.getMap("expirable");
    }

    @Test
    public void expirable_map_has_higher_storage_cost_than_regular_map() {
        for (int i = 0; i < 1_000; i++) {
            map.set(i, i);
        }

        for (int i = 0; i < 1_000; i++) {
            mapHasExpirableKeys.set(i, i, 10, TimeUnit.MINUTES);
        }

        HiDensityStorageInfo mapStorageInfo = getHiDensityStorageInfo(map);
        HiDensityStorageInfo mapHasExpirableKeysStorageInfo = getHiDensityStorageInfo(mapHasExpirableKeys);

        assertTrue(mapHasExpirableKeysStorageInfo.getUsedMemory() > mapStorageInfo.getUsedMemory());
    }

    @Test
    public void expirable_map_has_zero_storage_cost_after_entries_are_expired() {
        for (int i = 0; i < 1_000; i++) {
            mapHasExpirableKeys.set(i, i, 1, TimeUnit.SECONDS);
        }

        sleepAtLeastSeconds(2);

        for (int i = 0; i < 1_000; i++) {
            // force expired entries to remove
            mapHasExpirableKeys.get(i);
        }

        HiDensityStorageInfo mapHasExpirableKeysStorageInfo = getHiDensityStorageInfo(mapHasExpirableKeys);
        assertEquals(0, mapHasExpirableKeysStorageInfo.getUsedMemory());
        assertEquals(0, mapHasExpirableKeysStorageInfo.getEntryCount());
    }

    private static HiDensityStorageInfo getHiDensityStorageInfo(IMap map) {
        MapContainer container = ((MapService) ((MapProxyImpl) map).getService())
                .getMapServiceContext().getMapContainer(map.getName());
        EnterpriseMapContainer mapContainer = (EnterpriseMapContainer) container;
        HiDensityStorageInfo storageInfo = mapContainer.getHDStorageInfo();
        return storageInfo;
    }
}
