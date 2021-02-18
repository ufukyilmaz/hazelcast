package com.hazelcast.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getSmallInstanceHDIndexConfig;
import static org.junit.Assert.assertEquals;


/**
 * Tests for unsupported usage of Bitmap indexes with HD-IMap.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDBitmapIndexNotSupportedTest extends HazelcastTestSupport {

    public static final String MAP_NAME = "BitmapIndexNativeMemoryMap";

    private TestHazelcastFactory hazelcastFactory;
    private HazelcastInstance instance;

    private IndexConfig indexConfig;

    @Before
    public void setup() {
        hazelcastFactory = new TestHazelcastFactory();
        instance = hazelcastFactory.newHazelcastInstance(getSmallInstanceHDIndexConfig());
        indexConfig = new IndexConfig(IndexType.BITMAP, "UnusedAttribute");
    }

    @After
    public void stopHazelcastInstances() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testBitmapWithNativeFromConfig() {
        MapConfig mapConfig = new MapConfig(MAP_NAME);
        mapConfig.addIndexConfig(indexConfig);
        mapConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        instance.getConfig().addMapConfig(mapConfig);

        Exception exception = assertThrows(InvalidConfigurationException.class, () -> instance.getMap(MAP_NAME));
        assertEquals("BITMAP indexes are not supported by NATIVE storage", exception.getMessage());
    }

    @Test
    public void testBitmapWithNativeFromAddIndex() {
        Exception exception =
                assertThrows(IllegalArgumentException.class, () -> instance.getMap(MAP_NAME).addIndex(indexConfig));
        assertEquals("BITMAP indexes are not supported by NATIVE storage", exception.getMessage());
    }
}
