package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.HDQueryBounceTerminateTest.PredicateImpl;
import static com.hazelcast.query.impl.HDGlobalIndexProvider.PROPERTY_GLOBAL_HD_INDEX_ENABLED;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class HDQueryBounceGlobalIndexTerminateTest extends HDQueryBounceTest {

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.setProperty(PROPERTY_GLOBAL_HD_INDEX_ENABLED.getName(), "true");
        config.getSerializationConfig().addDataSerializableFactory(42, typeId -> new PredicateImpl());
        return config;
    }

    @Override
    protected boolean useTerminate() {
        return true;
    }

}
