package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class HDQueryDuringMigrationsStressTest extends QueryDuringMigrationsStressTest {

    @Override
    protected Config getConfig() {
        Config config = HDTestSupport.getHDConfig();
        MapConfig mapConfig = config.getMapConfig("employees");
        mapConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        return config;
    }

}
