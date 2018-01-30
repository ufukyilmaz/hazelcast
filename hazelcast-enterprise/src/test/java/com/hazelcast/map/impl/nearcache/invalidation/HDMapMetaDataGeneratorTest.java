package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.HDTestSupport;
import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDMapMetaDataGeneratorTest extends MapMetaDataGeneratorTest {

    @Override
    protected Config getConfig() {
        return HDTestSupport.getHDConfig(super.getConfig());
    }
}
