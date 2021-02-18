package com.hazelcast.client.map;

import com.hazelcast.HDTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assertThrows;
import static org.junit.Assert.assertEquals;

/**
 * Tests for unsupported usage of Bitmap indexes with HD-IMap in client side.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDClientBitmapIndexNotSupportedTest {
    private static final String MAP_NAME = "map";

    private TestHazelcastFactory hazelcastFactory;
    private HazelcastInstance client;

    IndexConfig indexConfig;

    @Before
    public void setup() {
        hazelcastFactory = new TestHazelcastFactory();
        // Start the member instance. Not a field because it isn't accessed after creation.
        // Use HDBitmapIndexNotSupportedTest if you want to test member instance.
        hazelcastFactory.newHazelcastInstance(HDTestSupport.getSmallInstanceHDIndexConfig());
        client = hazelcastFactory.newHazelcastClient();
        indexConfig = new IndexConfig(IndexType.BITMAP, "UnusedAttribute");
    }

    @After
    public void stopHazelcastInstances() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testBitmapWithNativeFromClientConfig() {
        MapConfig mapConfig = new MapConfig(MAP_NAME);
        mapConfig.addIndexConfig(indexConfig);
        mapConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        client.getConfig().addMapConfig(mapConfig);

        Exception exception = assertThrows(InvalidConfigurationException.class, () -> client.getMap(MAP_NAME));
        assertEquals("BITMAP indexes are not supported by NATIVE storage", exception.getMessage());
    }

    @Test
    public void testBitmapWithNativeFromClientAddIndex() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> client.getMap(MAP_NAME).addIndex(indexConfig));
        assertEquals("BITMAP indexes are not supported by NATIVE storage", exception.getMessage());
    }

}
