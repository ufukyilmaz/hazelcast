package com.hazelcast.internal.eviction;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.NativeMemoryTestUtil.assertFreeNativeMemory;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(NightlyTest.class)
public class HDMapExpirationStressTest extends MapExpirationStressTest {

    @Override
    protected Config getConfig() {
        return getHDConfig(super.getConfig());
    }

    @Override
    protected MapConfig getMapConfig() {
        return super.getMapConfig().setInMemoryFormat(InMemoryFormat.NATIVE);
    }

    @Override
    protected void assertRecords(HazelcastInstance[] instances) {
        super.assertRecords(instances);
        assertFreeNativeMemory(instances);
    }
}
