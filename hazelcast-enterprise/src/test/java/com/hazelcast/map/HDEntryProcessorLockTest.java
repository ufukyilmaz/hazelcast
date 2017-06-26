package com.hazelcast.map;


import com.hazelcast.HDTestSupport;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDEntryProcessorLockTest extends EntryProcessorLockTest {

    @Override
    public Config getConfig() {
        Config config = HDTestSupport.getHDConfig();
        MapConfig mapConfig = new MapConfig(MAP_NAME);
        mapConfig.setInMemoryFormat(inMemoryFormat);
        config.addMapConfig(mapConfig);
        return config;
    }

}
