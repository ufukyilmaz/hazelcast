package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDEntryProcessorOffloadableTest extends EntryProcessorOffloadableTest {

    @Parameterized.Parameters(name = "{index}: {0} sync={1} async={2}")
    public static Collection<Object[]> data() {
        return asList(new Object[][]{
                {BINARY, 0, 0}, {OBJECT, 0, 0}, {NATIVE, 0, 0},
                {BINARY, 1, 0}, {OBJECT, 1, 0}, {NATIVE, 1, 0},
                {BINARY, 0, 1}, {OBJECT, 0, 1}, {NATIVE, 0, 1},
        });
    }

    @Override
    public Config getConfig() {
        Config config = getHDConfig(super.getConfig());
        MapConfig mapConfig = new MapConfig(MAP_NAME);
        mapConfig.setInMemoryFormat(inMemoryFormat);
        mapConfig.setAsyncBackupCount(asyncBackupCount);
        mapConfig.setBackupCount(syncBackupCount);
        config.addMapConfig(mapConfig);
        return config;
    }

    @BeforeClass
    public static void setupClass() {
        System.setProperty(StandardMemoryManager.PROPERTY_DEBUG_ENABLED, "true");
    }

    @AfterClass
    public static void tearDownClass() {
        System.setProperty(StandardMemoryManager.PROPERTY_DEBUG_ENABLED, "false");
    }
}
