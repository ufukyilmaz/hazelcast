package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.internal.memory.StandardMemoryManager;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.POOLED;
import static com.hazelcast.query.impl.HDGlobalIndexProvider.PROPERTY_GLOBAL_HD_INDEX_ENABLED;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDEntryProcessorTest extends EntryProcessorTest {

    @Parameterized.Parameters(name = "format:{0} globalIndex:{1}")
    public static Collection<Object[]> data() {
        return asList(new Object[][]{
                {NATIVE, "true"},
                {NATIVE, "false"},
        });
    }

    @Parameterized.Parameter(1)
    public String globalIndex;

    @BeforeClass
    public static void setupClass() {
        System.setProperty(StandardMemoryManager.PROPERTY_DEBUG_ENABLED, "true");
    }

    @AfterClass
    public static void tearDownClass() {
        System.setProperty(StandardMemoryManager.PROPERTY_DEBUG_ENABLED, "false");
    }

    @Override
    public Config getConfig() {
        MapConfig mapConfig = new MapConfig(MAP_NAME)
                .setInMemoryFormat(inMemoryFormat);

        Config config = getHDConfig()
                .addMapConfig(mapConfig);
        config.getNativeMemoryConfig().setAllocatorType(POOLED);
        config.setProperty(PROPERTY_GLOBAL_HD_INDEX_ENABLED.getName(), globalIndex);
        return config;
    }

    @Override
    boolean globalIndex() {
        return globalIndex.equals("true");
    }

    @Override
    public void test_executeOnEntriesWithPredicate_runsOnBackup_whenIndexesAvailable() {
        super.test_executeOnEntriesWithPredicate_runsOnBackup_whenIndexesAvailable();
    }
}
